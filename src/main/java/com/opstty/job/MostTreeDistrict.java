package com.opstty.job;

import com.opstty.utils.DistrictAndCountWritable;
import com.opstty.mapper.MostTreeDistrictFirstMapper;
import com.opstty.mapper.MostTreeDistrictSecondMapper;
import com.opstty.reducer.MostTreeDistrictFirstReducer;
import com.opstty.reducer.MostTreeDistrictSecondReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MostTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: most <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job1 = Job.getInstance(conf, "most-stage1");
        job1.setJarByClass(MostTreeDistrict.class);
        job1.setMapperClass(MostTreeDistrictFirstMapper.class);
        job1.setReducerClass(MostTreeDistrictFirstReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(NullWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(LongWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        Path tempfile = new Path("/tmp/most-tree-districts-s1-" + System.currentTimeMillis());
        FileOutputFormat.setOutputPath(job1, tempfile);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "most-stage2");
        job2.setJarByClass(MostTreeDistrict.class);
        job2.setMapperClass(MostTreeDistrictSecondMapper.class);
        job2.setReducerClass(MostTreeDistrictSecondReducer.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(DistrictAndCountWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job2, tempfile);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));

        boolean job2success = job2.waitForCompletion(true);
        FileSystem.get(conf).delete(tempfile, true);
        System.exit(job2success ? 0 : 1);
    }
}
