package com.opstty.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class CsvMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Text, KEYOUT, VALUEOUT> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return;
        }

        try {
            TreeRow treeRow = new TreeRow(value.toString());
            this.mapRow(treeRow, context);
        } catch (NumberFormatException ignored) {
        }
    }

    public abstract void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException;
}
