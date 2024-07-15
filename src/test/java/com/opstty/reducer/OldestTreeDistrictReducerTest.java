package com.opstty.reducer;

import com.opstty.utils.DistrictAndYearWritable;
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
public class OldestTreeDistrictReducerTest {
    @Mock
    private Reducer.Context context;
    private OldestTreeDistrictReducer oldestTreeDistrictReducer;

    @Before
    public void setup() {
        this.oldestTreeDistrictReducer = new OldestTreeDistrictReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        NullWritable key = NullWritable.get();

        Iterable<DistrictAndYearWritable> values = Arrays.asList(
                new DistrictAndYearWritable(7, 2000),
                new DistrictAndYearWritable(8, 1900),
                new DistrictAndYearWritable(9, 1800)
        );

        this.oldestTreeDistrictReducer.reduce(key, values, this.context);
        verify(this.context).write(new IntWritable(9), NullWritable.get());
    }
}
