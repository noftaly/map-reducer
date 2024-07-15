package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictsReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictsReducer districtsReducer;

    @Before
    public void setup() {
        this.districtsReducer = new DistrictsReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(7);
        NullWritable value = NullWritable.get();

        Iterable<NullWritable> values = Arrays.asList(value, value, value);

        this.districtsReducer.reduce(key, values, this.context);
        verify(this.context).write(key, NullWritable.get());
    }
}
