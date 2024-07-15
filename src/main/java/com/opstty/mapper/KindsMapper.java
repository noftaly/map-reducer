package com.opstty.mapper;

import com.opstty.utils.CsvMapper;
import com.opstty.utils.TreeRow;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class KindsMapper extends CsvMapper<Text, IntWritable> {
    @Override
    public void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException {
        context.write(
                new Text(treeRow.getKind()),
                new IntWritable(1)
        );
    }
}
