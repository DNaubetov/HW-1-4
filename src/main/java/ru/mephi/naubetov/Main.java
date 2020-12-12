package ru.mephi.naubetov;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import ru.mephi.naubetov.hadoop.HW1Combiner;
import ru.mephi.naubetov.hadoop.HW1Partitioner;
import ru.mephi.naubetov.hadoop.HW1Mapper;
import ru.mephi.naubetov.hadoop.HW1Reducer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

public class Main {

    private static int REDUCER_COUNT = 3;

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: hadoopApp inputFilesDirectory resultDirectory [cacheFilePath]");
            return;
        }

        JobConf conf = new JobConf(Main.class);
        conf.setJobName("Binding price count");

        // Указываем классы с реализацией Mapper, Reducer и Partitioner
        conf.setMapperClass(HW1Mapper.class);
        conf.setCombinerClass(HW1Combiner.class);
        conf.setReducerClass(HW1Reducer.class);
        conf.setPartitionerClass(HW1Partitioner.class);

        // Если в аргументах есть файл для кэширования, то добавляем его в распределенных кэш
        if(args.length > 2) {
            DistributedCache.addCacheFile(new URI(args[2]), conf);
        }

        // Указываем путь до входных и выходных данных
        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));


        // Устанавливаем формат файла результата
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        //Настройка формата snappy encoding
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf,CompressionType.BLOCK);

        // Указываем формат ключа и значения результата
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Устанавливаем количество Reducer
        conf.setNumReduceTasks(REDUCER_COUNT);

        Instant startJobTime = Instant.now();
        JobClient.runJob(conf);
        Instant finishJobTime = Instant.now();
        System.out.println("LOG [INFO] Execution time: " + Duration.between(startJobTime, finishJobTime).toMillis() + " ms");
    }
}
