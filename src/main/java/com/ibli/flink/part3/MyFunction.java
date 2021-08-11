package com.ibli.flink.part3;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午7:46
 * @Version 1.0
 */
public class MyFunction implements MapFunction<String,Integer> {
    public Integer map(String s) throws Exception {
        return Integer.valueOf(s);
    }


    public void myfunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> objects = Lists.newArrayList();
        objects.add("1");
        objects.add("2");
        DataStreamSource<String> source = env.fromCollection(objects);
        SingleOutputStreamOperator<Integer> map = source.map(new MyFunction());
        map.print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> objects = Lists.newArrayList();
        objects.add("1");
        objects.add("2");
        objects.add("3");
        objects.add("5");
        DataStreamSource<String> source = env.fromCollection(objects);
        SingleOutputStreamOperator<Integer> map = source
                .filter(new FilterFunction<String>() {
                    public boolean filter(String s) throws Exception {
                        return !"1".equals(s);
                    }
                })
                .map(new RichMapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.valueOf(s);
            }
        });
        map.print();
        env.execute();
    }
}
