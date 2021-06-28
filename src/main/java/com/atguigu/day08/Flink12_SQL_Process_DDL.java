package com.atguigu.day08;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Flink12_SQL_Process_DDL {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO: 在建表语句中指定处理时间
        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        tableEnv.executeSql("select * from sensor").print();

    }
}
