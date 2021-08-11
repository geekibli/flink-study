package com.ibli.flink.part3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午5:49
 * @Version 1.0
 */
public class SlidingWindow {


    public static void main(String[] args) {
        // 设置执行环境, 类似spark中初始化SparkContext
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();

                int random = new Random().nextInt(10);
                System.err.println("value : " + value + " random : " + random + " timestamp : " + timeMillis + "|" + format.format(timeMillis));

                return new Tuple2<String, Integer>(value, random);
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);

        //基于时间驱动，每隔5s计算一下最近10s的数据
        //   WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(10), Time.seconds(5));
        //基于事件驱动，每隔2个事件，触发一次计算，本次窗口的大小为3，代表窗口里的每种事件最多为3个
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(3, 2);

        //   timeWindow.sum(1).print();

        countWindow.sum(1).print();

        //   timeWindow.apply(new MyTimeWindowFunction()).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
