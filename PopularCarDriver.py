package com.projectgurukul.wc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PopularCarDriver{
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		if (args.length !=3) {
			System.out.println("Usage: Popular Car Driving <input path> <intermediate path><output path>");
			System.exit(-1);
		}
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Car Sales Count");
        job1.setJarByClass(PopularCarDriver.class);
        job1.setMapperClass(CountMapper.class);
        job1.setCombinerClass(CountReducer.class);
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
        	System.exit(1);
        }
        
        // Job 2: Sort cars by sales
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort Cars by Sales");
        job2.setJarByClass(PopularCarDriver.class);
        job2.setMapperClass(KeySwapperMapper.class);
        job2.setSortComparatorClass(IntComparator.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
