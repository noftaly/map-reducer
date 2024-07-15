package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictsMapperTest {
    @Mock
    private Mapper.Context context;
    private DistrictsMapper districtsMapper;

    @Before
    public void setup() {
        this.districtsMapper = new DistrictsMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text header = new Text("geo;arr;kind;species;family;year;height;circumference;address;name;variety;objectid");
        Text row1 = new Text("geo;7;Maclura;species;family;2000;10.0;circumference;address;name;variety;objectid");
        Text row2 = new Text("geo;8;Calocedrus;species;family;2000;10.0;circumference;address;name;variety;objectid");
        Text row3 = new Text("geo;8;Pterocarya;species;family;2000;10.0;circumference;address;name;variety;objectid");

        this.districtsMapper.map(null, header, this.context);
        this.districtsMapper.map(null, row1, this.context);
        this.districtsMapper.map(null, row2, this.context);
        this.districtsMapper.map(null, row3, this.context);

        verify(this.context, times(1))
                .write(new IntWritable(7), NullWritable.get());

        verify(this.context, times(2))
                .write(new IntWritable(8), NullWritable.get());
    }
}
