import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MapReduce {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			
			String[] line = str.split("\t");
			long count = Long.parseLong(line[1]);
			int t = context.getConfiguration().getInt("t", 2);
			if(count <= t) {
				return;
			}
			String[] words = line[0].split(" ");
			context.write(new Text(line[0]), new Text("@" + count));
			if(words.length == 1) {
				return;
			}
			
			StringBuilder sb = new StringBuilder("");
			for(int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]);
				sb.append(" ");
			}
			String new_str = sb.toString().trim();
			context.write(new Text(new_str), new Text(words[words.length - 1] + "@" + count));
		}
	}
	
	public static class MyReducer extends TableReducer<Text, Text, Object>{
		
		public static final byte[] FAMILY = Bytes.toBytes("f1");
		public static final String TABLE_NAME = "table";
		
		public class Tuple {
			public String value;
			public long count;
		}
		
		public class Sort implements Comparator<Tuple>{
			public int compare(Tuple one, Tuple two) {
				if(one.count == two.count) {
					return 0;
				} else if(one.count < two.count) {
					return 1;
				} else {
					return -1;
				}
			}
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			
			long base = 0;
			for(Text value : values) {
				String cur = value.toString();
				String[] words = cur.split("@");
				long cur_count = Long.parseLong(words[1]);
				if(words[0].equals("")) {
					base = cur_count;
				} else {
					Tuple cur_tuple = new Tuple();
					cur_tuple.value = words[0];
					cur_tuple.count = cur_count;
					tuples.add(cur_tuple);
				}
			}
			
			if(base == 0 || tuples.size() == 0) {
				return;
			}
			Sort s = new Sort();
			Collections.sort(tuples, s);
			int n = context.getConfiguration().getInt("n", 5);
			Put put = new Put(Bytes.toBytes(key.toString()));
			for(int i = 0; i < n && i < tuples.size(); i++) {
				Tuple top = tuples.get(i);
				String predict_phrase = top.value;
				double predict_probability = top.count * 1.0 / base;
				String prob = String.valueOf(predict_probability);
				put.add(FAMILY, Bytes.toBytes(predict_phrase), Bytes.toBytes(prob));
			}
			context.write(null, put);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "MapReduce"); 
		job.setJarByClass(MapReduce.class);
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		TableMapReduceUtil.initTableReducerJob("table", MyReducer.class, job);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		conf.set("n", args[1]);
		conf.set("t", args[2]);
		
		job.waitForCompletion(true);
	}
}