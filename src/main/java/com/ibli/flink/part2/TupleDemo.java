package com.ibli.flink.part2;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午4:21
 * @Version 1.0
 */
public class TupleDemo {
    //对于 Java，Flink 自带有 Tuple0 到 Tuple25 类型。
    public static void main(String[] args) {
        Tuple2<String, Integer> fred = Tuple2.of("Fred", 35);
        String name = fred.f0;
        Integer age = fred.f1;
        System.err.println("name => " + name + " age => " + age);
    }
}
