package com.atguigu.day02.transform;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink08_Transform_FilterMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

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

        // TODO: filter
        SingleOutputStreamOperator<WaterSensor> filter = flatMap.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return "sensor_2".equals(waterSensor.getId());
            }
        });

        filter.print();
        env.execute();
    }
    public static class MyRichFlatMap extends RichFlatMapFunction<String,
            WaterSensor>{
        @Override
        public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {

        }
    }
}
