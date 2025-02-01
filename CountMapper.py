package com.projectgurukul.wc;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text carName = new Text();
	private IntWritable sales = new IntWritable();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Processing line: " + value.toString());
    	String[] tokens = value.toString().split("\t");
    	if (tokens.length == 2) {
    		carName.set(tokens[0].trim());
    		try {
				sales.set(Integer.parseInt(tokens[1].trim()));
    			context.write(carName, sales);
    		} catch (NumberFormatException e) {
    			System.err.println("Error parsing sales data: " + tokens[1]);
    		}
    	} else {
    		System.err.println("Invalid record: " + value.toString());
    	}
       
    }
}
