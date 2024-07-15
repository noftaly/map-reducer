package com.opstty.mapper;

import com.opstty.job.Tallest;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
public class TallestMapperTest {
    @Mock
    private Mapper.Context context;
    private TallestMapper tallestMapper;

    @Before
    public void setup() {
        this.tallestMapper = new TallestMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text header = new Text("geo;arr;kind;species;family;year;height;circumference;address;name;variety;objectid");
        Text row1 = new Text("geo;7;Calocedrus;species;family;2000;13.0;circumference;address;name;variety;objectid");
        Text row2 = new Text("geo;8;Maclura;species;family;2000;20.5;circumference;address;name;variety;objectid");
        Text row3 = new Text("geo;9;Maclura;species;family;2000;22.0;circumference;address;name;variety;objectid");

        this.tallestMapper.map(null, header, this.context);
        this.tallestMapper.map(null, row1, this.context);
        this.tallestMapper.map(null, row2, this.context);
        this.tallestMapper.map(null, row3, this.context);

        verify(this.context, times(1))
                .write(new Text("Calocedrus"), new FloatWritable(13f));

        verify(this.context, times(1))
                .write(new Text("Maclura"), new FloatWritable(20.5f));

        verify(this.context, times(1))
                .write(new Text("Maclura"), new FloatWritable(22f));
    }
}
