package com.ibli.flink.part3;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午5:19
 * @Version 1.0
 */
public class MyTimeWindowFunction implements WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {

    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        int sum = 0;

        for (Tuple2<String, Integer> tuple2 : input) {
            sum += tuple2.f1;
        }

        long start = window.getStart();
        long end = window.getEnd();

        out.collect("key:" + tuple.getField(0) + " value: " + sum + "| window_start :"
                + format.format(start) + "  window_end :" + format.format(end)
        );

    }
}