/*
 * 15619 Project 4.1
 * @author: jiali Chen
 * Step 1: generate ngrams from Project Gutenberg 
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	//Convert all words to lower-case.
    	String line = value.toString().replaceAll("[^a-zA-Z]", " ").toLowerCase();
      StringTokenizer itr = new StringTokenizer(line);
      List<String> words = new ArrayList<String>();
      while (itr.hasMoreTokens()) {
    	  words.add(itr.nextToken());
         
        }
      for (int i=0; i<words.size(); i++) {
    	  StringBuffer gram = new StringBuffer(words.get(i));
    	  word.set(gram.toString());
    	  context.write(word, one);
    	  for (int len=i+1; len <i+5 && len<words.size(); len++) {
    		  gram.append(" "+words.get(len));
    		  word.set(gram.toString());
        	  context.write(word, one);
    		  
    	  }
    	  
      }

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}