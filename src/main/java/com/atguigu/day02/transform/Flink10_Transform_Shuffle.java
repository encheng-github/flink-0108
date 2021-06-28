package com.atguigu.day02.transform;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink10_Transform_Shuffle {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //flatmap没有返回值,所以是一进多出
        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value,
                                Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.valueOf(split[1]),
                        Integer.parseInt(split[2])));
            }
        });

        // TODO: shuffle
        DataStream<WaterSensor> shuffle = flatMap.shuffle();


        shuffle.print();
        env.execute();
    }
}
