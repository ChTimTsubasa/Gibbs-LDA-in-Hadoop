package mapred.LDA;

import java.io.IOException;
import java.util.Random;

import mapred.util.Tokenizer;
import mapred.util.IntArrayWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

/**
 * Perform docs gathering
 * @author Tianyi Chen
 *
 */
public class GibbsSampleMapper extends Mapper<LongWritable, Text,
        IntWritable, IntArrayWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        InputParser parser = new InputParser(value.toString());
        int docID = parser.getKey();
        int[] word_topic = parser.getVal();
        int gatherID = docID % 17;
//        System.out.println(gatherID);
        int[] doc_word_topic = new int[word_topic.length + 1];
        doc_word_topic[0] = docID;
        System.arraycopy(word_topic, 0,
                        doc_word_topic, 1, word_topic.length);

        if (gatherID >= 17){
            System.out.println("ERROR writing gather ID "+gatherID);
            return;
        }

        context.write(new IntWritable(gatherID), new IntArrayWritable(doc_word_topic));
    }

}