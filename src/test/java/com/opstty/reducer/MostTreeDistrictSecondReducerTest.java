package com.opstty.reducer;

import com.opstty.utils.DistrictAndCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MostTreeDistrictSecondReducerTest {
    @Mock
    private Reducer.Context context;
    private MostTreeDistrictSecondReducer mostTreeDistrictReducer;

    @Before
    public void setup() {
        this.mostTreeDistrictReducer = new MostTreeDistrictSecondReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        NullWritable key = NullWritable.get();

        Iterable<DistrictAndCountWritable> values = Arrays.asList(
                new DistrictAndCountWritable(1, 5),
                new DistrictAndCountWritable(2, 10),
                new DistrictAndCountWritable(3, 15)
        );

        this.mostTreeDistrictReducer.reduce(key, values, this.context);
        verify(this.context).write(new IntWritable(3), NullWritable.get());
    }
}
