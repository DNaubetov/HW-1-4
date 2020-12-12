package ru.mephi.naubetov;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.mephi.naubetov.hadoop.HW1Mapper;
import ru.mephi.naubetov.hadoop.HW1Reducer;
import ru.mephi.naubetov.util.InputGenerate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MRTest {

    MapDriver<LongWritable, Text, IntWritable, IntWritable> mapDriver;
    ReduceDriver<IntWritable, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setup() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper_ValidRecord() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(InputGenerate.getValidRecord()))
                .withOutput(new IntWritable(222), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapper_InvalidRecord() throws IOException {
        List<Pair<IntWritable, IntWritable>> results = mapDriver
                .withInput(new LongWritable(), new Text(InputGenerate.getInvalidRecord()))
                .run();
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new IntWritable(222), values)
                .withOutput(new Text("222"), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(InputGenerate.getValidRecord()))
                .withInput(new LongWritable(), new Text(InputGenerate.getValidRecord()))
                .withInput(new LongWritable(), new Text(InputGenerate.getInvalidRecord()))
                .withOutput(new Text("222"), new IntWritable(2))
                .runTest();
    }
}
