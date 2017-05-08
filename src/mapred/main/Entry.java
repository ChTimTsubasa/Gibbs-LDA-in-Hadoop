package mapred.main;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import mapred.LDA.DataInitMapper;
import mapred.LDA.DataInitReducer;
import mapred.LDA.WordCombineMapper;
import mapred.LDA.WordCombineReducer;
import mapred.LDA.GibbsSampleMapper;
import mapred.LDA.GibbsSampleReducer;

import mapred.util.IntIntArrayInputFormat;

import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import mapred.util.IntArrayWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

/**
 * Entry class for LDA.
 * @author Tianyi Chen
 */
public class Entry {
    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String input = parser.get("input");
        String output = parser.get("output");
        int numOfTopic = parser.getInt("numOfTopic");
        int numOfIteration = parser.getInt("numOfIteration");
        int numOfWord = parser.getInt("numOfWord");
        float alpha = alpha = 50.0f / ((float) numOfTopic);
        float beta = 0.01f;

        System.out.println("word num "+numOfWord);

        System.out.println("Running program " + "..");


        long start = System.currentTimeMillis();




        System.out.println("Initialize data");
        dataInit(input, output + "Data_doc_0", numOfTopic);
        wordCombine(output + "Data_doc_0", output + "Data_word_0",
                    numOfTopic, numOfWord);
        System.out.println("Start Training");
        for (int i = 0; i < numOfIteration; i++) {
            System.out.println("====Iteration: " + (i + 1) + "====");
            GibbsSample(output + "Data_doc_" + i,
                        output + "Data_doc_" + (i+1),
                        output + "Data_word_" + i,
                        alpha, beta, numOfTopic, numOfWord);
            wordCombine(output + "Data_doc_" + (i+1),
                        output + "Data_word_" +(i+1),
                        numOfTopic, numOfWord);
        }


        long end = System.currentTimeMillis();


        System.out.println(String.format("Runtime for program: %d ms",
                end - start));

    }

    private static void GibbsSample(String input, String output,
                                    String wordInfo,
                                    float alpha, float beta,
                                    int numOfTopic, int numOfWord)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setFloat("alpha", alpha);
        conf.setFloat("beta", beta);
        conf.set("wordInfo", wordInfo);
        conf.setInt("numOfTopic", numOfTopic);
        conf.setInt("numOfWord", numOfWord);
        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Gibbs Sampling");
        job.setClasses(GibbsSampleMapper.class, GibbsSampleReducer.class,
                null);
        job.setMapOutputClasses(IntWritable.class, IntArrayWritable.class);
        job.run();
    }

    private static void dataInit(String input, String output, int numOfTopic)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("numOfTopic", numOfTopic);
        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Initialize Docs");
        job.setClasses(DataInitMapper.class, DataInitReducer.class,
                DataInitReducer.class);
        job.setMapOutputClasses(IntWritable.class, IntArrayWritable.class);
        job.run();
    }

    private static void wordCombine(String input, String output, int numOfTopic, int numOfWord)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("numOfTopic", numOfTopic);
        conf.setInt("numOfWord", numOfWord);
        Optimizedjob job = new Optimizedjob(conf, input, output,
                "Combine words");
        job.setClasses(WordCombineMapper.class, WordCombineReducer.class,
                WordCombineReducer.class);
        job.setMapOutputClasses(IntWritable.class, IntArrayWritable.class);
        job.run();
    }
}
