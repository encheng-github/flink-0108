package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink14_SQL_EventTime_DDL {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO: 在建表语句中指定处理时间
        tableEnv.executeSql(
                "create table sensor(" +
                        "id string," +
                        "ts bigint," +
                        "vc int, " +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        "watermark for t as t - interval '5' second)" +
                        "with("
                        + "'connector' = 'filesystem',"
                        + "'path' = 'input/sensor-sql.txt',"
                        + "'format' = 'csv'"
                        + ")"
        );

        tableEnv.executeSql("select * from sensor").print();
    }
}
