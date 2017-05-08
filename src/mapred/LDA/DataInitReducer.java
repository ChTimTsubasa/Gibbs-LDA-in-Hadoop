package mapred.LDA;

import java.io.IOException;
import java.util.LinkedList;

import mapred.util.Tokenizer;
import mapred.util.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * Reduce and combine docID - [word, topic, word, topic...]
 * @author Tianyi Chen
 */
public class DataInitReducer extends Reducer<IntWritable, IntArrayWritable,
                                            IntWritable, IntArrayWritable> {
    @Override
    protected void reduce(IntWritable docID, Iterable<IntArrayWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        LinkedList<Integer> list = new LinkedList<>();
        for (IntArrayWritable intArray : values) {
            int[] ints = intArray.getArray();
            for (int ele : ints) {
                list.add(ele);
            }
        }
        int[] allInts = new int[list.size()];
        int i = 0;
        for (int ele : list) {
            allInts[i] = ele;
            i++;
        }

        context.write(docID, new IntArrayWritable(allInts));
    }
}
