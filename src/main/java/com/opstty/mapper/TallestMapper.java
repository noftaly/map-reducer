package com.opstty.mapper;

import com.opstty.utils.CsvMapper;
import com.opstty.utils.TreeRow;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TallestMapper extends CsvMapper<Text, FloatWritable> {
    @Override
    public void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException {
        context.write(
                new Text(treeRow.getKind()),
                new FloatWritable(treeRow.getHeight())
        );
    }
}
