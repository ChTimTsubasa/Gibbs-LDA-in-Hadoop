package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	public static int n = 1;
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);
		if (words.length < n){
			return;
		}
		for (int i = 0; i <= words.length - n; i++) {
			StringBuilder word = new StringBuilder(words[0]);
			for (int j = i + 1; j < i + n; j++) {
				word.append(" " + words[j]);
			}
			context.write(new Text(word.toString().trim()), NullWritable.get());
		}
	}
}
