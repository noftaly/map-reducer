package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
public class TallestReducerTest {
    @Mock
    private Reducer.Context context;
    private TallestReducer tallestReducer;

    @Before
    public void setup() {
        this.tallestReducer = new TallestReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Text key = new Text("pomifera");

        Iterable<FloatWritable> values = Arrays.asList(new FloatWritable(5), new FloatWritable(10), new FloatWritable(20));

        this.tallestReducer.reduce(key, values, this.context);
        verify(this.context).write(key, new FloatWritable(20));
    }
}
