package com.atguigu.day02.transform;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink09_Transform_KeyBy {
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

        // TODO: keyby
        //方式一:使用字段名(局限于javaBean)
//        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy("id");
        //方式二:使用下标方式(适用于Tuple)
//        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy(0);
        //方式三:自定义分区
        KeyedStream<WaterSensor, String> keyedStream = flatMap.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });


        keyedStream.print();
        env.execute();
    }
}
