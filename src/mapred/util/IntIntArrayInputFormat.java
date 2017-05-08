package mapred.util;

import java.io.IOException;

import mapred.util.IntArrayWritable;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Custom input type for Mapper with
 * IntWritable as key and IntArray as value.
 * @author Tianyi Chen
 */
public class IntIntArrayInputFormat extends
        FileInputFormat<IntWritable, IntArrayWritable> {

    @Override
    public RecordReader<IntWritable, IntArrayWritable>
            createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        return new IntIntArrayRecordReader();
    }
}