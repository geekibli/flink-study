package com.ibli.flink.part5;

import com.ibli.flink.part4.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author gaolei
 * @Date 2021/8/12 下午3:01
 * @Version 1.0
 */
public class RichFunctionDemo {

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

        DataStream<Tuple2<String, Integer>> stream = dataStream.map(new MyMapFunction());
        stream.print();
        env.execute();
    }

    public static class MyMapFunction extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.err.println("invoke open");
            // 一般定义状态，或者链接数据库操作
            super.open(parameters);
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            RuntimeContext runtimeContext = this.getRuntimeContext();
            System.err.println("runtimeContext.getTaskName() : " + runtimeContext.getTaskName());
            return new Tuple2<>(sensorReading.getSersorId(), runtimeContext.getIndexOfThisSubtask());
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.err.println("invoke close method");
        }
    }
}
