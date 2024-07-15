package com.opstty.mapper;

import com.opstty.utils.CsvMapper;
import com.opstty.utils.TreeRow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SpeciesMapper extends CsvMapper<Text, NullWritable> {
    public void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException {
        context.write(
                new Text(treeRow.getSpecies()),
                NullWritable.get()
        );
    }
}
