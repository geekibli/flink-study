package com.ibli.flink.part5;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author gaolei
 * @Date 2021/8/12 下午3:35
 * @Version 1.0
 */
public class PartitionDemo {

    public static StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    //广播分区会将上游数据输出到下游算子的每个实例中。适合于大数据集和小数据集做Jion的场景
    public static void broadcast() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<String> dataStream = env.fromElements("hhh", "ggg", "fff", "ddd", "sss", "aaa", "qqq", "www");
        DataStream<String> broadcast = dataStream.broadcast();
        broadcast.print("broadcast : ");
        env.execute();
    }

    //数据会被随机分发到下游算子的每一个实例中进行
    public static void shuffle() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<String> dataStream = env.fromElements("hhh", "ggg", "fff", "ddd", "sss", "aaa", "qqq", "www");
        DataStream<String> broadcast = dataStream.shuffle();
        broadcast.print("shuffle : ");
        env.execute();
    }

    //用于将记录输出到下游本地的算子实例。它要求上下游算子并行度一样。简单的说，ForwardPartitioner用来做数据的控制台打印。
    public static void forward() throws Exception {
        StreamExecutionEnvironment env = getEnv().setParallelism(1);
        DataStreamSource<String> dataStream = env.fromElements("hhh", "ggg", "fff", "ddd", "sss", "aaa", "qqq", "www");
        DataStream<String> broadcast = dataStream.shuffle();
        broadcast.print("shuffle : ");
        env.execute();
    }

    /**
     * 这种分区器会根据上下游算子的并行度，循环的方式输出到下游算子的每个实例。这里有点难以理解，假设上游并行度为 2，编号为 A 和 B。下游并行度为 4，编号为 1，2，3，4。那么 A 则把数据循环发送给 1 和 2，B 则把数据循环发送给 3 和 4。假设上游并行度为 4，编号为 A，B，C，D。下游并行度为 2，编号为 1，2。那么 A 和 B 则把数据发送给 1，C 和 D 则把数据发送给 2。
     */
    public static void rescale() throws Exception {
        StreamExecutionEnvironment env = getEnv().setParallelism(4);
        DataStreamSource<String> dataStream = env.fromElements("hhh", "ggg", "fff", "ddd", "sss", "aaa", "qqq", "www");
        dataStream.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s + "_**";
            }
        }).setParallelism(1).rescale().print("rescale : ");

        env.execute();
    }


    public static void rebalance() throws Exception {
        StreamExecutionEnvironment env = getEnv().setParallelism(4);
        DataStreamSource<String> dataStream = env.fromElements("hhh", "ggg", "fff", "ddd", "sss", "aaa", "qqq", "www");
        dataStream.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s + "_**";
            }
        }).setParallelism(1).rebalance().print("rebalance : ");

        env.execute();
    }

    //  数据会被分发到下游算子的第一个实例中进行处理
    public static void global() throws Exception {
        StreamExecutionEnvironment env = getEnv().setMaxParallelism(8);
        DataStreamSource<String> dataStream = env.fromElements("hhh", "ggg", "fff", "ddd", "sss", "aaa", "qqq", "www");
        dataStream.flatMap(new RichFlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s + "_**");
            }
        }).setParallelism(2).global().print("global : ");

        env.execute();
    }

    public static void keyBy() throws Exception {
        StreamExecutionEnvironment env = getEnv().setMaxParallelism(8);
        DataStreamSource<String> dataStream = env.fromElements("hhh", "hhh", "hhh", "hhh", "sss", "sss", "sss", "www");
        dataStream.flatMap(new RichFlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s + "_**");
            }
        }).keyBy(String::toString).print("keyBy : ");

        env.execute();
    }

    public static void custom() throws Exception {
        StreamExecutionEnvironment env = getEnv().setMaxParallelism(8);
        DataStreamSource<String> dataStream = env.fromElements("hhhh", "hhhss", "hhh", "hhh", "sss", "sss", "sss", "www");
        dataStream.flatMap(new RichFlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s + "_**");
            }
        }).partitionCustom(new CustomPartitioner(),String::toString)
        .print("custom :");

        env.execute();
    }


    public static class CustomPartitioner implements Partitioner<String> {
        // key: 根据key的值来分区
        // numPartitions: 下游算子并行度
        @Override
        public int partition(String key, int numPartitions) {
            return key.length() % numPartitions;//在此处定义分区策略
        }
    }


    public static void main(String[] args) throws Exception {
        custom();
    }


}
