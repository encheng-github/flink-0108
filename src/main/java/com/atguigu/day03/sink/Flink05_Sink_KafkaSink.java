package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Flink05_Sink_KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map =
                streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return JSON.toJSONString(waterSensor);
            }
        });

        // TODO: KafkaSink
        map.addSink(new FlinkKafkaProducer<String>("hadoop102:9092",
                "flink-0108",new SimpleStringSchema()));
        env.execute();
    }
}
