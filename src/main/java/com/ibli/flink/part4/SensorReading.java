package com.ibli.flink.part4;

import java.io.Serializable;

/**
 * @Author gaolei
 * @Date 2021/8/11 下午4:13
 * @Version 1.0
 */
public class SensorReading implements Serializable {
    private String sersorId;
    private double timestamp;
    private double newtemp;

    public SensorReading() {
    }

    public SensorReading(String sersorId, double timestamp, double newtemp) {
        this.sersorId = sersorId;
        this.timestamp = timestamp;
        this.newtemp = newtemp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sersorId='" + sersorId + '\'' +
                ", timestamp=" + timestamp +
                ", newtemp=" + newtemp +
                '}';
    }

    public String getSersorId() {
        return sersorId;
    }

    public void setSersorId(String sersorId) {
        this.sersorId = sersorId;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    public double getNewtemp() {
        return newtemp;
    }

    public void setNewtemp(double newtemp) {
        this.newtemp = newtemp;
    }
}
