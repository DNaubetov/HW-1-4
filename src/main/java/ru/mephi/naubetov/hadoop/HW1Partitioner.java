package ru.mephi.naubetov.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class HW1Partitioner implements Partitioner<IntWritable, IntWritable> {

    /**
     * Получить номер раздела для данного ключа
     * @param numReduceTasks Общее количество партиншнеров
     * @return Номер партишнера
     */
    @Override
    public int getPartition(IntWritable key, IntWritable value, int numReduceTasks) {
        return(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
    /**
     * Инициализирует новый экземпляр из JobConf.
     * @param conf Конфигурация job
     */
    @Override
    public void configure(JobConf conf) {
    }
}
