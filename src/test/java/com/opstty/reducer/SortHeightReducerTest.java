package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
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
public class SortHeightReducerTest {
    @Mock
    private Reducer.Context context;
    private SortHeightReducer sortHeightReducer;

    @Before
    public void setup() {
        this.sortHeightReducer = new SortHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        FloatWritable key = new FloatWritable(20f);

        Iterable<Text> values = Arrays.asList(new Text("123"), new Text("456"), new Text("789"));

        this.sortHeightReducer.reduce(key, values, this.context);
        verify(this.context).write(new Text("123"), key);
    }
}
