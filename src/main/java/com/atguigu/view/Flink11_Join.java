package com.atguigu.view;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink11_Join {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.端口读取数据创建两个流
        DataStreamSource<String> streamSource1 = env.socketTextStream(
                "hadoop102", 8888);
        DataStreamSource<String> streamSource2 = env.socketTextStream(
                "hadoop102", 9999);

        //3.转化为JavaBean对象并提取时间戳生成waterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = streamSource1.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]),
                    Integer.parseInt(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000L;
            }
        }));

        SingleOutputStreamOperator<WaterSensor> waterSensorDS2 =
                streamSource2.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]),
                    Integer.parseInt(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000L;
            }
        }));

        //4.JOIN
        SingleOutputStreamOperator<Tuple2<WaterSensor, WaterSensor>> result = waterSensorDS1.keyBy(WaterSensor::getId)
                .intervalJoin(waterSensorDS2.keyBy(WaterSensor::getId))
                .between(Time.seconds(-5), Time.seconds(5))//生产环境,如果不想丢任何数据,
                // 则写最大延迟
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor, Tuple2<WaterSensor, WaterSensor>>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor right, Context ctx, Collector<Tuple2<WaterSensor, WaterSensor>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        //5.打印数据
        result.print();

        //6.启动
        env.execute();

    }
}
