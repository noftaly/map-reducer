package com.opstty.mapper;

import com.opstty.utils.DistrictAndCountWritable;
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
public class MostTreeDistrictSecondMapperTest {
    @Mock
    private Mapper.Context context;
    private MostTreeDistrictSecondMapper mostTreeDistrictMapper;

    @Before
    public void setup() {
        this.mostTreeDistrictMapper = new MostTreeDistrictSecondMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text row1 = new Text("7\t5");
        Text row2 = new Text("8\t10");
        Text row3 = new Text("9\t15");

        this.mostTreeDistrictMapper.map(null, row1, this.context);
        this.mostTreeDistrictMapper.map(null, row2, this.context);
        this.mostTreeDistrictMapper.map(null, row3, this.context);

        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAndCountWritable(7, 5));

        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAndCountWritable(8, 10));

        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAndCountWritable(9, 15));
    }
}
