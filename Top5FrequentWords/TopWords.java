import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.*;

public class TopWords extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		//private String inputFile;
		private String content;
		private Text artifactid = new Text();
		private Text word = new Text();
		IntWritable one = new IntWritable(1);
		String searchterm;

		public void configure(JobConf job) {
			//inputFile = job.get("mapreduce.map.input.file");
			searchterm = job.get("searchterm");

		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] splitstrings = new String[5];
			splitstrings = line.split("\t");
			artifactid.set(splitstrings[0]);
			String title = splitstrings[1];
			String maincontent = splitstrings[3];

			content = title + " " + maincontent;

			if (title.contains(searchterm)) {
				StringTokenizer tokenizer = new StringTokenizer(content);
				while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					output.collect(word, one);
				}

			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, IntWritable,Text> {
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<IntWritable,Text> output, Reporter reporter)
				throws IOException {

			int sum1 = 0;
			while (values.hasNext()) {
				sum1 += values.next().get();
			}
			output.collect(new IntWritable(sum1),key);
		}

	}

	public static class Combiner extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			output.collect(key, new IntWritable(sum));
		}
	}

	public static class Map1 extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {

//private String inputFile;


public void map(LongWritable key, Text value,
		OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException {
	
String[] args=value.toString().split("\t");
System.out.println(args[0] + "," + args[1]);
output.collect(new LongWritable(Long.valueOf(args[0])),new Text( args[1]));

}
}

public static class Reduce1 extends MapReduceBase implements
	Reducer<LongWritable,Text, LongWritable,Text> {
	int counter = 1;
public void reduce(LongWritable key, Iterator<Text> values,
		OutputCollector<LongWritable,Text> output, Reporter reporter)
		throws IOException {
	Text valuecombined = new Text();

	while (values.hasNext()) {
		valuecombined.set(valuecombined.toString() +"," + values.next().toString());
	}
	
	if(counter <= 5)
	{
	output.collect(key,valuecombined);
	counter++;
	}
}

}

	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), TopWords.class);
		JobConf conf2 = new JobConf(getConf(), TopWords.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combiner.class);
		conf.setReducerClass(Reduce.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.set("searchterm", args[2]);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("temp"));

		
		conf2.setJobName("Sorting");
		conf2.setOutputKeyClass(LongWritable.class);
		conf2.setOutputValueClass(Text.class);
		
		conf2.setMapperClass(Map1.class);
		conf2.setCombinerClass(Reduce1.class);
		conf2.setReducerClass(Reduce1.class);
		
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		conf2.setOutputKeyComparatorClass(ReverseComparator.class);
		FileInputFormat.setInputPaths(conf2, new Path("temp"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
		/*Job job1=new Job(conf); 
		Job job2=new Job(conf2);*/
		
		/* JobControl jbcntrl=new JobControl("jbcntrl"); 
		 jbcntrl.addJob(job1); 
		 jbcntrl.addJob(job2);
		 job2.addDependingJob(job1); 
		 jbcntrl.run();*/
		
		JobClient.runJob(conf);
		JobClient.runJob(conf2);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopWords(), args);
		System.exit(res);
	}
	
	public static class ReverseComparator extends WritableComparator {
	     
	    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
	    public ReverseComparator() {
	        super(LongWritable.class);
	    }
	 
	    @Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	       return (-1)* TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
	    }
	 
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable a, WritableComparable b) {
	        if (a instanceof Text && b instanceof Text) {
	                return (-1)*(((Text) a).compareTo((Text) b));
	        }
	        return super.compare(a, b);
	    }
	}
}
