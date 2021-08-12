package com.ibli.flink.part4;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @Author gaolei
 * @Date 2021/8/11 下午4:10
 * @Version 1.0
 */
public class SourceFromSelfDefind {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource dataStreamSource = env.addSource(new MySourceFunction());
        dataStreamSource.print();
        env.execute();
    }

    // 实现自定义的source
    public static class MySourceFunction implements SourceFunction<SensorReading> {
        // 定义标识位 控制数据产生
        private boolean running = true;

        public void run(SourceContext ctx) throws Exception {
            // 定义各个随机数生成器
            HashMap<String, Double> sensorMap = new HashMap<String, Double>(10);
            for (int i = 0; i < 10; i++) {
                sensorMap.put("sensor_" + (i + 1), 60 + new Random().nextGaussian() * 20);
            }
            while (running) {
                for (String sensor : sensorMap.keySet()) {
                    double newtemp = sensorMap.get(sensor) + new Random().nextGaussian();
                    sensorMap.put(sensor, newtemp);
                    ctx.collect(new SensorReading(sensor, System.currentTimeMillis(), newtemp));
                }
                Thread.sleep(10000);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
