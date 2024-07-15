package com.opstty.reducer;

import com.opstty.utils.DistrictAndCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreeDistrictSecondReducer extends Reducer<NullWritable, DistrictAndCountWritable, IntWritable, NullWritable> {
    public void reduce(NullWritable key, Iterable<DistrictAndCountWritable> values, Context context) throws IOException, InterruptedException {
        int mostTreeDistrict = 0;
        int maxCount = 0;
        for (DistrictAndCountWritable value : values) {
            if (value.getCount() > maxCount) {
                mostTreeDistrict = value.getDistrict();
                maxCount = value.getCount();
            }
        }

        context.write(new IntWritable(mostTreeDistrict), NullWritable.get());
    }
}
