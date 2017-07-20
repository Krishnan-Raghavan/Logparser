import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyClass {
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line=value.toString();
			String s[]=line.split(",");
			double l = 0;
			
			
			for(int i=0;i<s.length;i++){
				String a=s[1];
				String n[]=a.split(":");
				
				
				l=(Integer.parseInt(n[0])*60)+Integer.parseInt(n[1])+(Integer.parseInt(n[2])/60);
			
				
				if(s[i].equals("DEBUG")  || s[i].equals("WARN") || s[i].equals("ERROR") || s[i].equals("FATAL") || s[i].equals("INFO"))
				{
					
				
					
					
					
					context.write(new Text(s[0]+"	"+Math.round(l)+"	"+s[i]), new IntWritable(1));}
			}
		}
	}
	
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
		
			int ans=0;
			for(IntWritable val:values){
				ans=ans+val.get();
			}
			context.write(key, new IntWritable(ans));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=new Job(conf, "Hi");
		
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		
		
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	

}
