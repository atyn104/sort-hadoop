package com.projectgurukul.wc;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        System.out.println("Reduce input key: "+ key.toString());
        for(IntWritable val : values){
            sum += val.get();
        }
        System.out.println("Reduce output: " + key.toString() +"\t" + sum);
        context.write(key, new IntWritable(sum));
    }
}
