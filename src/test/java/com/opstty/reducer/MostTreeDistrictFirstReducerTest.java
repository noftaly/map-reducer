package com.opstty.reducer;

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
public class MostTreeDistrictFirstReducerTest {
    @Mock
    private Reducer.Context context;
    private MostTreeDistrictFirstReducer mostTreeDistrictReducer;

    @Before
    public void setup() {
        this.mostTreeDistrictReducer = new MostTreeDistrictFirstReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(1);

        Iterable<NullWritable> values = Arrays.asList(NullWritable.get(), NullWritable.get(), NullWritable.get());

        this.mostTreeDistrictReducer.reduce(key, values, this.context);
        verify(this.context).write(new IntWritable(1), new IntWritable(3));
    }
}
