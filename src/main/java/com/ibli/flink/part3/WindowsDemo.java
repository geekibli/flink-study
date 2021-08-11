package com.ibli.flink.part3;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午4:56
 * @Version 1.0
 */
public class WindowsDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> stones = env.fromElements(
                new Person("Bob",23),
                new Person("Fred",19)
        );

        SingleOutputStreamOperator<Person> stream = stones.filter(new FilterFunction<Person>() {
            public boolean filter(Person person) throws Exception {
                return person.getAge() > 10;
            }
        });

        GlobalWindows globalWindows = GlobalWindows.create();
        // 处理每秒钟的数据
        TumblingEventTimeWindows timeWindows = TumblingEventTimeWindows.of(Time.seconds(1));
        // 每5分钟处理前1个小时的数据
        SlidingEventTimeWindows slidingEventTimeWindows = SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5));
        // 每个会话的网页浏览量，其中会话之间的间隔至少为30分钟
        EventTimeSessionWindows.withGap(Time.minutes(30));


    }
}
