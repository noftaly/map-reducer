package com.opstty.reducer;

import com.opstty.utils.DistrictAndYearWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeDistrictReducer extends Reducer<NullWritable, DistrictAndYearWritable, IntWritable, NullWritable> {
    public void reduce(NullWritable key, Iterable<DistrictAndYearWritable> values, Context context) throws IOException, InterruptedException {
        int oldestTreeDistrict = 0;
        int minYear = Integer.MAX_VALUE;
        for (DistrictAndYearWritable value : values) {
            if (value.getYear() < minYear) {
                oldestTreeDistrict = value.getDistrict();
                minYear = value.getYear();
            }
        }

        context.write(new IntWritable(oldestTreeDistrict), NullWritable.get());
    }
}
