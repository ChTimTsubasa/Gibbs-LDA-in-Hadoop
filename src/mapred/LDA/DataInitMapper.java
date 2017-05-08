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
 * Map docID wordID count into docID - [word, topic, word, topic...]
 * @author Tianyi Chen
 */
public class DataInitMapper extends Mapper<LongWritable, Text,
                                IntWritable, IntArrayWritable> {
    private int numOfTopics;
    private int docID;
    private int wordID;
    private int wordCount;
    private Random random;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        numOfTopics = conf.getInt("numOfTopic",0);
        String line = value.toString();
        String[] words = line.split(" ");
        int[] values;
        random = new Random();
        if (words.length < 3) {
            return;
        }

        docID = Integer.parseInt(words[0]) - 1;
        wordID = Integer.parseInt(words[1]) - 1;
        wordCount = Integer.parseInt(words[2]);
        IntWritable outKey = new IntWritable(docID);

        for (int i = 0; i < wordCount; i++) {
            values = new int[2];
            values[0] = wordID;
            values[1] = random.nextInt(numOfTopics);
            context.write(outKey, new IntArrayWritable(values));
        }
    }

}