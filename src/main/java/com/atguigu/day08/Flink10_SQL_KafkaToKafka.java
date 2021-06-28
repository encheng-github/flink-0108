package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink10_SQL_KafkaToKafka {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO: 1.注册SourceKafka的表
        tableEnv.executeSql("create table sourceKafka (" +
                "id string," +
                "ts bigint," +
                "vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'properties.group.id' = 'atguigu',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")"
        );

        // TODO: 2.注册SinkKafka的表
        tableEnv.executeSql("create table sinkKafka(id string, ts bigint, vc " +
                "int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'format' = 'csv'"
                + ")"
        );

        // 3. 从SourceTable 查询数据, 并写入到 SinkTable
        tableEnv.executeSql("insert into sinkKafka select * from sourceKafka " +
                "where id = 'sensor_1'");
    }
}
