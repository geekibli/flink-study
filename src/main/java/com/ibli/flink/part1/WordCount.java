package com.ibli.flink.part1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author gaolei
 * @Date 2021/7/4 10:36 下午
 * 批处理 WordCount 程序
 * @Version 1.0
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2、读取文件数据
        String inputPath = "/Users/gaolei/Documents/DemoProjects/flink-start/src/main/resources/hello.txt";
        DataSource<String> dataSource = env.readTextFile(inputPath);
        // 对数据集进行处理 按照空格分词展开 转换成（word，1）二元组
        AggregateOperator<Tuple2<String, Integer>> result = dataSource.flatMap(new MyFlatMapper())
                // 按照第一个位置 -> word 分组
                .groupBy(0)
                .sum(1);
        result.print();
    }


    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            // 首先按照空格分词
            String[] words = s.split(" ");
            // 遍历所有的word 包装成二元组输出
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
