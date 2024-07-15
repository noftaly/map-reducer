package com.opstty.mapper;

import com.opstty.utils.CsvMapper;
import com.opstty.utils.TreeRow;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class DistrictsMapper extends CsvMapper<IntWritable, NullWritable> {
    @Override
    public void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(treeRow.getDistrict()), NullWritable.get());
    }
}
