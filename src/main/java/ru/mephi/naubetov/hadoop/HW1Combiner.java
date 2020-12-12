package ru.mephi.naubetov.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.util.Iterator;

public class HW1Combiner extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    /**
     * Объедините обработанные записи
     * @param key Ключ обработанных записей
     * @param values Значения обработанных записей
     * @param output Сборщик выходных данных
     * @param reporter Репортер прогресса и счетчики обновлений, информация о состоянии и т.д.
     */
    @Override
    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) {
        try {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }

            output.collect(key, new IntWritable(sum));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
