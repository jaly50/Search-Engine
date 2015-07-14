import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Step2 {
	static class  WordPair {
		public WordPair(String word, int count) {
			this.word = word;
			this.count = count;
		}

		String word;
		int count;
	}

	public static class Step2Mapper extends
	//Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
			Mapper<LongWritable, Text, Text, Text> {

	
		private Text word = new Text();


		private int t;

		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			System.out.println("in the setup");
			conf = context.getConfiguration();
			// property value as an int, or defaultValue.
			
			t = conf.getInt("t", 2);

		}

		@Override
		//  1st Parameter: keyin 2nd Parameter: valuein
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			System.out.println("from map "+line);
			// word-count key value pair
			String pair[] = line.split("\t");
			int count = Integer.parseInt(pair[1]);

			// ignore phrases that appear below a certain threshold, say t
			if (count <= t)
				return;

			String words[] = pair[0].split(" ");

			// Or if it is n-gram, there is no predict word, so ignore it as
			// well
			if (words.length == 5)
				return;

			// output the original words and its counts
			String first = pair[0];
			word.set(first);
			Text countable = new Text(pair[1]);
			context.write(word, countable);

			// If there is only one word, it can not be other's predicting word
			if (words.length == 1)
				return;

			// Get phrase of its previous one
			int spaceindx = pair[0].lastIndexOf(" ");
			String phrase = pair[0].substring(0, spaceindx);
			word.set(phrase);
			Text text = new Text(words[words.length - 1] + " " + pair[1]);
			context.write(word, text);

		}
	}

	public static class Step2Reducer extends
	// TableReducer<KEYIN,VALUEIN,KEYOUT>
			TableReducer<Text, Text, ImmutableBytesWritable> {

		final byte[] tableName = Bytes.toBytes("table");
		static final byte[] family = Bytes.toBytes("f1");

		private Configuration conf;
		private int n;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			conf = context.getConfiguration();
			// property value as an int, or defaultValue.
			n = conf.getInt("n", 5);

		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
			int originCount = 0;

			PriorityQueue<WordPair> queue = new PriorityQueue<WordPair>(n,
					comparator);

			// Put top n words-probabilities into queue
			for (Text val : values) {
				String parts[] = val.toString().split(" ");
				// the original phrase
				if (parts.length == 1) {
					originCount = Integer.parseInt(parts[0]);
				}
				// phrase +word
				else {
					int count = Integer.parseInt(parts[1]);
					String word = parts[0];
					WordPair wp = new WordPair(word, count);
					if (queue.size() > n) {
						WordPair queuetop = queue.peek();
						if (queuetop.count < count) {
							queue.poll();
							queue.add(wp);
						}
					} else {
						queue.add(wp);
					}
				}
			}

			byte[] keybyte = Bytes.toBytes(key.toString());
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(keybyte);

			Put put = new Put(rowKey.get());

			for (WordPair wp : queue) {
				put.add(family, Bytes.toBytes(wp.word),
						Bytes.toBytes((double) wp.count / originCount));
			}

			context.write(null, put);
		}

	}

	private static Comparator<WordPair> comparator = new Comparator<WordPair>() {

		@Override
		public int compare(WordPair a, WordPair b) {
			if (a.count > b.count)
				return 1;
			else if (a.count < b.count)
				return -1;
			return 0;
		}
	};

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		System.out.println("args is: "+Arrays.toString(args));
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		System.out.println("remaining args is: "+Arrays.toString(remainingArgs));
		if (remainingArgs.length != 2 && remainingArgs.length != 6) {
			System.err
					.println("Usage: Step2 <in> <out> [-t threshold] [-n topN]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "step2");
		job.setJarByClass(Step2.class);
		
		job.setMapperClass(Step2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < remainingArgs.length; ++i) {
			if ("-n".equals(remainingArgs[i])) {
				int n = Integer.parseInt(remainingArgs[i + 1]);
				System.out.println("n is: "+n);
				job.getConfiguration().setInt("n", n);
			} else if ("-t".equals(remainingArgs[i])) {
				int t = Integer.parseInt(remainingArgs[i + 1]);
				System.out.println("t is: "+t);
			
				job.getConfiguration().setInt("t", t);
			}

			else {
				otherArgs.add(remainingArgs[i]);
			}
		}

		TableMapReduceUtil.initTableReducerJob("table", // output table
				Step2Reducer.class, // reducer class
				job);
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		
		job.waitForCompletion(true) ;

	}
}