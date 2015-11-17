import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

	
	public class Male7 {
		public static class Map
		extends Mapper<LongWritable, Text, Text, Text>{
		private final static Text one = new Text(" ");
	//	private Text word = new Text(); // type of output key
		public void map(LongWritable key, Text value, Context context
		) throws IOException, InterruptedException {
		String line = value.toString(); // line to string token
		String tokens[]=line.split("::");
		Text userid = new Text(tokens[0]);
		String gender=tokens[1];
		int age= Integer.parseInt(tokens[2]);
		if(age <= 7 && gender.equalsIgnoreCase("M"))
		{
			context.write(userid, one); // create a pair <keyword, 1>
//			word.set(userid); // set word as each input keyword
		}
		
		}
		}
		public static class Reduce
		extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text(" ");
		public void reduce(Text key, Iterable<Text> values,
		Context context
		) throws IOException, InterruptedException {
	int sum = 0; // initialize the sum for each keyword
//		for (IntWritable val : values) {
//		sum += val.get();
//		}
		result.set(" ");
		context.write(key, result); // create a pair <keyword, number of occurences>
		}
		}
		// Driver program
		public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
		System.err.println("Usage: WordCount <in> <out>");
		System.exit(2);
		}
		// create a job with name "wordcount"
		Job job = new Job(conf, "male7");
		job.setJarByClass(Male7.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		}


