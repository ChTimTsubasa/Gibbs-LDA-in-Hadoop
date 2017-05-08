package mapred.LDA;

import java.io.IOException;
import java.util.LinkedList;

import mapred.util.Tokenizer;
import mapred.util.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 * Combine the words info
 * @author Tianyi Chen
 */
public class WordCombineReducer extends Reducer<IntWritable, IntArrayWritable,
        IntWritable, IntArrayWritable> {
    @Override
    protected void reduce(IntWritable wordID, Iterable<IntArrayWritable> topicCounts,
                          Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numOfTopic = conf.getInt("numOfTopic",0);
        int [] allTopicCount = new int[numOfTopic];
        for (IntArrayWritable topicCount : topicCounts) {
            int[] tc = topicCount.getArray();
            for (int i = 0; i < numOfTopic; i++) {
                allTopicCount[i] += tc[i];
            }
        }

        context.write(wordID, new IntArrayWritable(allTopicCount));
    }
}
