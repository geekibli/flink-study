package com.ibli.flink.part3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午8:07
 * @Version 1.0
 */
public class IterationDemo {

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        iteration.print();

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        test();
    }
}
