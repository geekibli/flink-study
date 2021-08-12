package com.ibli.flink.part5;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author gaolei
 * @Date 2021/8/12 下午1:58
 * @Version 1.0
 */
public class WindowJoinDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> dataStreamSource = env.fromElements(new Person("Fred", 23), new Person("John", 30));
        DataStream<Person> dataStream1 = dataStreamSource.map(data -> {
            return data;
        });
        DataStreamSource<Person> personDataSource = env.fromElements(new Person("Fred", 50), new Person("Jerry", 19));
        DataStream<Person> dataStream2 = personDataSource.map(data -> {
            return data;
        });

        dataStreamSource.join(personDataSource)
                .where(new KeySelector<Person, Person>() {
                    @Override
                    public Person getKey(Person person) throws Exception {
                        String name = person.getName();
                        System.err.println(person.toString());
                        return new Person(name + "Smith",person.getAge());
                    }
                });

    }
}
