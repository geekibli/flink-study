package com.ibli.flink.part4;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @Author gaolei
 * @Date 2021/8/11 下午2:37
 * @Version 1.0
 * 学习一下StreamApi 提供的各种算子
 */
public class SuanziDemo {

    private static StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }


    public static void mapTest() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        ArrayList<Integer> nums = Lists.newArrayList();
        nums.add(1);
        nums.add(2);
        nums.add(3);
        DataStreamSource<Integer> source = env.fromCollection(nums);
        SingleOutputStreamOperator<Integer> map = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }
        });
        map.print();
        env.execute();
    }

    public static void keyByTest() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(
                new Tuple2<String, Integer>("age", 1),
                new Tuple2<String, Integer>("name", 2),
                new Tuple2<String, Integer>("name", 3),
                new Tuple2<String, Integer>("name", 3));

        source.map(
                new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        Integer f1 = stringIntegerTuple2.f1;
                        stringIntegerTuple2.setField(f1 + 10, 1);
                        return stringIntegerTuple2;
                    }
                })
                .keyBy(1)
                .print();
        env.execute();
    }

    public static void reduceTest() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        env.fromElements(
                Tuple2.of(2L, 3L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L))
                .keyBy(1)
                .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> t1) throws Exception {
                        return new Tuple2<Long, Long>(t1.f0, longLongTuple2.f1 + t1.f1);
                    }
                })
                .print();
        env.execute();
    }


    public static void reduce() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 9999);

        DataStream<SensorReading> dataStream = dataSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));
            }
        });

        DataStream<SensorReading> sersorId = dataStream.keyBy("sersorId")
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                        String id = t1.getSersorId();
                        Double time = t1.getTimestamp();
                        return new SensorReading(id, time, Math.max(sensorReading.getNewtemp(), t1.getNewtemp()));
                    }
                });
        sersorId.print();
        env.execute();
    }


    public static void splitTest() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 9999);

        DataStream<SensorReading> dataStream = dataSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0], new Double(strings[1]), new Double(strings[2]));
            }
        });

        SplitStream<SensorReading> split = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getNewtemp() > 30) ? Collections.singleton("high") : Collections.singleton("low");
            }
        });

        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> all = split.select("high", "low");

        // connect
        DataStream<Tuple2<String, Double>> highStream = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getSersorId(), sensorReading.getNewtemp());
            }
        });

        // 链接之后的stream
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = highStream.connect(low);

        SingleOutputStreamOperator<Object> resultStream = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f0, "high temp warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getSersorId(), "normal temp");
            }
        });

        resultStream.print();
        env.execute();
    }


    public static void unionTest() throws Exception {
        // 必须是数据类型相同
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> firstStream = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> secondStream = env.socketTextStream("localhost", 7777);


        DataStream<String> unionStream = firstStream.union(secondStream);
        DataStream<SensorReading> dataStream = unionStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0], new Double(strings[1]), new Double(strings[2]));
            }
        });
        dataStream.print();
        env.execute();
    }


    public static void main(String[] args) throws Exception {
        unionTest();
    }
}
