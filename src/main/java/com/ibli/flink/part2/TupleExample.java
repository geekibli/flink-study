package com.ibli.flink.part2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午4:23
 * @Version 1.0
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/learn-flink/datastream_api/
 */
public class TupleExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> stones = env.fromElements(
                new Person("Bob", 11),
                new Person("John", 22),
                new Person("Fred", 33)
        );

        DataStream<Person> adults = stones.filter(new FilterFunction<Person>() {
            public boolean filter(Person person) throws Exception {
                return person.getAge() > 18;
            }
        });

        adults.print();
        env.execute();
    }

}
