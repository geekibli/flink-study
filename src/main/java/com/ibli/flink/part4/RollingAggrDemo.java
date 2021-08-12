package com.ibli.flink.part4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author gaolei
 * @Date 2021/8/11 下午4:47
 * @Version 1.0
 */
public class RollingAggrDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSource = env.socketTextStream("localhost", 9999);

        DataStream<SensorReading> dataStream = dataSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0], new Long(strings[1]), new Double(strings[2]));
            }
        });
//        dataStream.keyBy("sersorId").max("timestamp").print("max : ");
        dataStream.keyBy("sersorId").maxBy("timestamp").print("max : ");
//        dataStream.keyBy("sersorId").min("timestamp").print("min : ");
        env.execute();

    }
}


























