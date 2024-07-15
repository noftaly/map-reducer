package com.opstty.mapper;

import com.opstty.utils.DistrictAndCountWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTreeDistrictSecondMapper extends Mapper<Object, Text, NullWritable, DistrictAndCountWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split("\t");
        context.write(
                NullWritable.get(),
                new DistrictAndCountWritable(
                        Integer.parseInt(columns[0]),
                        Integer.parseInt(columns[1])
                )
        );
    }
}
