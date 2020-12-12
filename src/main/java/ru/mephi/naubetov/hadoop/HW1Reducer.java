package ru.mephi.naubetov.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HW1Reducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, Text, IntWritable> {

    private static Map<Integer, String> cityMapCache = new HashMap<>();
//  Объединение данных
    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) {
        try {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            // Получаем наименование города по id из кэша
            String city = cityMapCache.get(key.get());
            if(city == null) {
                city = key.toString();
            }

            output.collect(new Text(city), new IntWritable(sum));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Установка редюсеров перед: инициализации распределеного кеш
     * @param conf Конфигурация job
     */
    @Override
    public void configure(JobConf conf) {
        try {
            File cachedFile = new File("city");
            if(cachedFile.exists()) {
                BufferedReader brReader = new BufferedReader(new FileReader(cachedFile));
                String strLineRead = "";
                while ((strLineRead = brReader.readLine()) != null) {
                    String[] cityMappings = strLineRead.split("\\s+");
                    cityMapCache.put(Integer.parseInt(cityMappings[0].trim()), cityMappings[1].trim());
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
