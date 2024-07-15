package com.opstty.mapper;

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
public class KindsMapperTest {
    @Mock
    private Mapper.Context context;
    private KindsMapper kindsMapper;

    @Before
    public void setup() {
        this.kindsMapper = new KindsMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text header = new Text("geo;arr;kind;species;family;year;height;circumference;address;name;variety;objectid");
        Text row1 = new Text("geo;7;Maclura;species;family;2000;10.0;circumference;address;name;variety;objectid");
        Text row2 = new Text("geo;8;Calocedrus;species;family;2000;10.0;circumference;address;name;variety;objectid");
        Text row3 = new Text("geo;8;Calocedrus;species;family;2000;10.0;circumference;address;name;variety;objectid");

        this.kindsMapper.map(null, header, this.context);
        this.kindsMapper.map(null, row1, this.context);
        this.kindsMapper.map(null, row2, this.context);
        this.kindsMapper.map(null, row3, this.context);

        verify(this.context, times(1))
                .write(new Text("Maclura"), new IntWritable(1));

        verify(this.context, times(2))
                .write(new Text("Calocedrus"), new IntWritable(1));
    }
}
