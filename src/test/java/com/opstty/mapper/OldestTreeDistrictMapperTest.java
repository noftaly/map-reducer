package com.opstty.mapper;

import com.opstty.utils.DistrictAndYearWritable;
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
public class OldestTreeDistrictMapperTest {
    @Mock
    private Mapper.Context context;
    private OldestTreeDistrictMapper oldestTreeDistrictMapper;

    @Before
    public void setup() {
        this.oldestTreeDistrictMapper = new OldestTreeDistrictMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text header = new Text("geo;arr;kind;species;family;year;height;circumference;address;name;variety;objectid");
        Text row1 = new Text("geo;7;Calocedrus;species;family;2000;10.0;circumference;address;name;variety;objectid");
        Text row2 = new Text("geo;8;Maclura;species;family;1900;10.0;circumference;address;name;variety;objectid");
        Text row3 = new Text("geo;9;Maclura;species;family;1800;10.0;circumference;address;name;variety;objectid");

        this.oldestTreeDistrictMapper.map(null, header, this.context);
        this.oldestTreeDistrictMapper.map(null, row1, this.context);
        this.oldestTreeDistrictMapper.map(null, row2, this.context);
        this.oldestTreeDistrictMapper.map(null, row3, this.context);

        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAndYearWritable(7, 2000));

        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAndYearWritable(8, 1900));

        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAndYearWritable(9, 1800));
    }
}
