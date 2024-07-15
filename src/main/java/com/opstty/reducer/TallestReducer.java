package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class TallestReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        double tallest = StreamSupport.stream(values.spliterator(), false)
                .mapToDouble(FloatWritable::get)
                .max()
                .orElse(Float.NEGATIVE_INFINITY);
        context.write(key, new FloatWritable((float) tallest));
    }
}
