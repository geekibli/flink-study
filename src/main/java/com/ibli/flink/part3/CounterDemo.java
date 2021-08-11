package com.ibli.flink.part3;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午7:58
 * @Version 1.0
 */
public class CounterDemo extends RichMapFunction<String, Integer> {
    private IntCounter numLines = new IntCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("count-test", this.numLines);
    }

    @Override
    public Integer map(String s) throws Exception {
        this.numLines.add(2);
        return Integer.valueOf(s);
    }

    public static void count() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> objects = Lists.newArrayList();
        objects.add("1");
        objects.add("2");
        DataStreamSource<String> source = env.fromCollection(objects);
        SingleOutputStreamOperator<Integer> map = source.map(new CounterDemo());
        map.print();
        JobExecutionResult execute = env.execute();
        Object accumulatorResult = execute.getAccumulatorResult("count-test");
        System.err.println("count-test  " + accumulatorResult);
    }


    public static void main(String[] args) throws Exception {
        count();
    }
}
