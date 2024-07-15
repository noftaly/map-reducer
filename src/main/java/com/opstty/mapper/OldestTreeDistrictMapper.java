package com.opstty.mapper;

import com.opstty.utils.CsvMapper;
import com.opstty.utils.DistrictAndYearWritable;
import com.opstty.utils.TreeRow;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class OldestTreeDistrictMapper extends CsvMapper<NullWritable, DistrictAndYearWritable> {
    @Override
    public void mapRow(TreeRow treeRow, Context context) throws IOException, InterruptedException {
        context.write(
                NullWritable.get(),
                new DistrictAndYearWritable(treeRow.getDistrict(), treeRow.getYear())
        );
    }
}
