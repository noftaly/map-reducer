package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
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
public class SortHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private SortHeightMapper sortHeightMapper;

    @Before
    public void setup() {
        this.sortHeightMapper = new SortHeightMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text header = new Text("geo;arr;kind;species;family;year;height;circumference;address;name;variety;objectid");
        Text row1 = new Text("geo;7;kind;species;family;2000;13.0;circonference;address;name;variety;123");
        Text row2 = new Text("geo;8;kind;species;family;2000;20.0;circonference;address;name;variety;456");
        Text row3 = new Text("geo;9;kind;species;family;2000;20.0;circonference;address;name;variety;789");

        this.sortHeightMapper.map(null, header, this.context);
        this.sortHeightMapper.map(null, row1, this.context);
        this.sortHeightMapper.map(null, row2, this.context);
        this.sortHeightMapper.map(null, row3, this.context);

        verify(this.context, times(1))
                .write(new FloatWritable(13f), new Text("123"));

        verify(this.context, times(1))
                .write(new FloatWritable(20f), new Text("456"));

        verify(this.context, times(1))
                .write(new FloatWritable(20f), new Text("789"));
    }
}
