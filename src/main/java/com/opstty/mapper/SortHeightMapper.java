package com.opstty.mapper;

import com.opstty.utils.CsvMapper;
import com.opstty.utils.TreeRow;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SortHeightMapper extends CsvMapper<FloatWritable, Text> {
    @Override
    public void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException {
        context.write(
                new FloatWritable(treeRow.getHeight()),
                new Text(treeRow.getObjectId())
        );
    }
}
