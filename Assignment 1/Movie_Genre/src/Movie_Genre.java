import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Movie_Genre {
	// Global variable to hold the command line argument passed for genre filtering
public static String  genre;
public static class Map
extends Mapper<LongWritable, Text, Text, Text>{
private final static Text one = new Text(" ");
private Text word = new Text(); // type of output key
public void map(LongWritable key, Text value, Context context
) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"::"); // line to string token
String line = value.toString(); // line to string token
String tokens[]=line.split("::");
Configuration conf = context.getConfiguration();
genre = conf.get("genre");
String genre_file=tokens[2];
String genre_list[]=genre_file.split("\\|");
Text title = new Text(tokens[1]);

for(int i=0;i<genre_list.length;i++)
{
if(genre_list[i].equalsIgnoreCase(genre))
{	context.write(title, one); // create a pair <keyword, 1>

}
}
}
}
public static class Reduce
extends Reducer<Text,Text,Text,Text> {
private Text result = new Text();
public void reduce(Text key, Iterable<Text> values,
Context context
) throws IOException, InterruptedException {

result.set(" ");
context.write(key, result); // create a pair <keyword, number of occurences>
}
}
// Driver program
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
// get all args
if (otherArgs.length != 3) {
System.err.println("Usage: Movie_Genre <in> <out> <genre>");
System.exit(2);
}

String gen=args[2];

if(!(gen.equalsIgnoreCase("Action")
		|| gen.equalsIgnoreCase("Adventure")
		|| gen.equalsIgnoreCase("Animation")
		|| gen.equalsIgnoreCase("Children's")
		|| gen.equalsIgnoreCase("Comedy")
		|| gen.equalsIgnoreCase("Crime")
		|| gen.equalsIgnoreCase("Documentary")
		|| gen.equalsIgnoreCase("Drama")
		|| gen.equalsIgnoreCase("Fantasy")
		|| gen.equalsIgnoreCase("Film-Noir")
		|| gen.equalsIgnoreCase("Horror")
		|| gen.equalsIgnoreCase("Musical")
		|| gen.equalsIgnoreCase("Mystery")
		|| gen.equalsIgnoreCase("Romance")
		|| gen.equalsIgnoreCase("Sci-Fi")
		|| gen.equalsIgnoreCase("Thriller")
		|| gen.equalsIgnoreCase("War")
		|| gen.equalsIgnoreCase("Western")))
{
	System.err.println("Usage: Not a valid genre name.. Please try with something else");
	System.exit(2);
}
conf.set("genre", args[2]);

// create a job with name "wordcount"
Job job = new Job(conf, "moviegenre");
job.setJarByClass(Movie_Genre.class);
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
