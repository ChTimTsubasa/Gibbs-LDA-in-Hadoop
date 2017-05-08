package mapred.util;


import java.io.IOException;

import mapred.util.IntArrayWritable;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Custom Record Reader
 * @author Tianyi Chen
 */
public class IntIntArrayRecordReader extends RecordReader<IntWritable, IntArrayWritable> {

    private LineRecordReader reader = new LineRecordReader();
    private IntWritable key;
    private IntArrayWritable value;

    public void initialize(InputSplit is, TaskAttemptContext tac)
            throws IOException, InterruptedException {
        reader.initialize(is, tac);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {    // get the next line
        if (!reader.nextKeyValue()) {
            return false;
        }
        Text lineValue = reader.getCurrentValue();
        // parse the lineValue which is in the format:
        // objName, x, y, z
//        System.out.println(lineValue.toString());
        String [] tokens = lineValue.toString().split("\\t");
        key = new IntWritable(Integer.parseInt(tokens[0]));


        // try to parse floating point components of value
        String[] items = tokens[1].replace(" ", "").replace(" ", "").replace(" ", "").split(",");

        int[] results = new int[items.length];

        for (int i = 0; i < items.length; i++) {
            try {
                results[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException nfe) {
                //NOTE: write something here if you need to recover from formatting errors
            };
        }

        value = new IntArrayWritable(results);

        return true;
    }

    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public IntArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    public void close() throws IOException {
        reader.close();
    }

    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}