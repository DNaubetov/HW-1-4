package ru.mephi.naubetov.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class HW1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private static Integer MIN_BID_PRICE = 250;
    private static IntWritable ONE = new IntWritable(1);

//    Преобразование входных записей в промежуточные записи

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) {
        try {
            String[] inputData = value.toString().split("\\t");

            // Фильтруем записи по значению поля bindingPrice
            if(Integer.parseInt(inputData[19]) > MIN_BID_PRICE) {
                output.collect(new IntWritable(Integer.parseInt(inputData[7])), ONE);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
