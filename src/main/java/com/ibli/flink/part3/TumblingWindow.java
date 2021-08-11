package com.ibli.flink.part3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午5:17
 * @Version 1.0
 */
public class TumblingWindow {

    public static void main(String[] args) {
        //设置执行环境，类似spark中初始化sparkContext
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {

            public Tuple2<String, Integer> map(String value) throws Exception {

                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                long timeMillis = System.currentTimeMillis();

                int random = new Random().nextInt(10);

                System.out.println("value: " + value + " random: " + random + " timestamp: " + timeMillis + " | " + format.format(timeMillis));

                return new Tuple2<String, Integer>(value, random);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);


        // 基于时间驱动，每隔10s划分一个窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(10));

        // 基于事件驱动, 每相隔3个事件(即三个相同key的数据), 划分一个窗口进行计算
        // WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(3);

        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。
        timeWindow.apply(new MyTimeWindowFunction()).print();
        // countWindow.apply(new MyCountWindowFunction()).print();

        try {
            // 转换算子都是lazy init的, 最后要显式调用 执行程序
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
