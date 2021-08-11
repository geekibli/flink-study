package com.ibli.flink.part4;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @Author gaolei
 * @Date 2021/8/10 下午8:12
 * @Version 1.0
 */
public class WatermarkDemo {

    public static void test(){
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<MyEvent> stream = env.readFile(
//                myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
//                FilePathFilter.createDefaultFilter(), typeInfo);
//
//        DataStream<MyEvent> withTimestampsAndWatermarks = stream
//                .filter( event -> event.severity() == WARNING )
//                .assignTimestampsAndWatermarks(<watermark strategy>);
//
//        withTimestampsAndWatermarks
//                .keyBy( (event) -> event.getGroup() )
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .reduce( (a, b) -> a.add(b) )
//                .addSink(...);
    }
}
