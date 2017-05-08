package mapred.LDA;

import java.io.IOException;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.Random;
import mapred.util.Tokenizer;
import mapred.util.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

/**
 * Perform Gibbs Sampling:
 * for each word, compute its topic distribution
 * and assign a topic.
 *
 * Requires Reading file from Word file.
 * @author Tianyi Chen
 */
public class GibbsSampleReducer extends Reducer<IntWritable, IntArrayWritable,
        IntWritable, IntArrayWritable> {
    @Override
    protected void reduce(IntWritable group_ID, Iterable<IntArrayWritable> doc_topic_counts,
                          Context context)
            throws IOException, InterruptedException {
        int groupID = group_ID.get();
        Configuration conf = context.getConfiguration();
        int numOfTopic = conf.getInt("numOfTopic", 0);
        int numOfWord = conf.getInt("numOfWord", 0);
        String wordInput = conf.get("wordInfo");
        double alpha = (double)conf.getFloat("alpha", 0.0f);
        double beta = (double)conf.getFloat("beta", 0.0f);

        Random random = new Random();

        //load necessary parameters
        int[][] nwt = new int[numOfWord][numOfTopic];
        int[] nt = new int[numOfTopic];
        partialLoad(wordInput, nwt, nt, conf);


        for (IntArrayWritable doc_topic_count : doc_topic_counts) {
            int []docTopicCount = doc_topic_count.getArray();
            int docID = docTopicCount[0];
            int[] ntd = calculateTopicInDoc(docID, docTopicCount, numOfWord, numOfTopic);
            double[] probs = new double[numOfTopic];
            for (int i = 1; i < docTopicCount.length; i += 2) {
                int word = docTopicCount[i];
                int topic = docTopicCount[i+1];

                ntd[topic]--;
                nt[topic]--;
                nwt[word][topic]--;

                computeProbability(word, ntd, nwt, nt,
                        numOfTopic, numOfWord, probs, alpha, beta);
                topic = getDistribution(probs, random);

                docTopicCount[i+1] = topic;
                nt[topic]++;
                nwt[word][topic]++;
            }
            int[] ouputDocTopicCount = new int[docTopicCount.length - 1 ];
            System.arraycopy(docTopicCount, 1,
                    ouputDocTopicCount , 0, ouputDocTopicCount.length);

            context.write(new IntWritable(docID), new IntArrayWritable(ouputDocTopicCount));
        }

    }

    private int[] calculateTopicInDoc(int docID, int[] docTopicCount, int numOfWord, int numOfTopic){
        int[] ntd = new int[numOfTopic];

        for (int i = 1; i < docTopicCount.length; i += 2) {

            int topic = docTopicCount[i+1];
            ntd[topic]++;
        }
        return ntd;
    }

    private void partialLoad(String wordInfo, int[][] nwt, int[] nt,Configuration conf) {
        try {
            Path path = new Path(conf.get(wordInfo));
            FileSystem fs = path.getFileSystem(conf);
            FileStatus[] statuses = fs.listStatus(path);
            for (FileStatus status : statuses){
                Path filePath = status.getPath();
                if (filePath.getName().startsWith(".") || filePath.getName().startsWith("_")){
                    continue;
                }
                BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(filePath)));
                String line = br.readLine();
                System.out.println(line);
                while (line != null) {
                    InputParser parser = new InputParser(line);
                    int word = parser.getKey();
                    nwt[word] = parser.getVal();
                    for (int i = 0; i < nwt[word].length; i++) {
                        nt[i] += nwt[word][i];
                    }
                    line = br.readLine();
                }
                br.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void computeProbability(int word,
        int [] ntd, int[][] nwt, int[] nt,
        int numOfTopic, int numOfWords,
        double[] probs,
        double alpha, double beta) {

        double norm = 0.0;
        for (int i = 0; i < numOfTopic; i++) {
            double pwt = ((double)nwt[word][i] + beta) / ((double)(nt[i]) + (double)numOfWords * beta);
            double ptd = ((double)ntd[i] + alpha);
            if (i == 0)
                probs[i] = pwt * ptd;
            else
                probs[i] = probs[i-1] + probs[i];
        }
        for (int i = 0; i < numOfTopic; i++) {
            probs[i] /= probs[numOfTopic-1];
        }
    }

    public int getDistribution(double [] probs, Random random) {
        double sample = random.nextDouble();
        for (int i = 0; i < probs.length; i++) {
            if (sample < probs[i]) {
                return i;
            }
        }
        return probs.length - 1;
    }
}
