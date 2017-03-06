package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by igor on 03.03.17.
 */
public class FilterReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        for (Text elem : value) {
            context.write(NullWritable.get(), elem);
        }
    }
}
