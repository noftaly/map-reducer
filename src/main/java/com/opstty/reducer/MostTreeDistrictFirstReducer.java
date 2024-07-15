package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreeDistrictFirstReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (NullWritable value : values) {
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}
