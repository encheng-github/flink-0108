package com.atguigu.day09;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink04_SQL_TumWindow {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(),
                $("vc"));

        // TODO: 开启一个3s的滚动窗口
        tableEnv.executeSql("select\n" +
                "  id,\n" +
                "  sum(vc) as vcSum,\n" +
                "  Tumble_start(ts,interval '3' second) as windowStart,\n" +
                "  Tumble_end(ts,interval '3' second) as windowEnd\n" +
                "from " +
                table +
                " group by \n" +
                "  id,tumble(ts,interval '3' second)").print();

    }
}
