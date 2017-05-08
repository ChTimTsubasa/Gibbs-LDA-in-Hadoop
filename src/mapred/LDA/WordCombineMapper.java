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
 * maps doc-[word, topic.....] into word[topic1Count, topic2Count...]
 * @author Tianyi Chen
 */
public class WordCombineMapper extends Mapper<LongWritable, Text,
        IntWritable, IntArrayWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numOfTopic = conf.getInt("numOfTopic",0);
        int numOfWord = conf.getInt("numOfWord",0);
        InputParser parser = new InputParser(value.toString());
        //int docID = parser.getKey();
        int[] word_topic = parser.getVal();
        int[][] word_topic_count = new int[numOfWord][];

        for (int i = 0; i < word_topic.length; i += 2) {
            int word = word_topic[i];
            int topic = word_topic[i+1];
            if (word_topic_count[word] == null) {
                word_topic_count[word] = new int[numOfTopic];
            }
            word_topic_count[word][topic]++;
        }

        for (int i = 0; i < numOfWord; ++i) {
            if (word_topic_count[i] == null) {
                continue;
            } else {
                context.write(new IntWritable(i), new IntArrayWritable(word_topic_count[i]));
            }
        }
    }

}