package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink12_Transform_Union {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> elem1 = env.fromElements("a", "b", "c", "d",
                "e");

        DataStreamSource<String> elem2 = env.fromElements("1", "2", "3", "4",
                "5");

        // TODO: Union
        DataStream<String> union = elem1.union(elem2);
        SingleOutputStreamOperator<String> map = union.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s + "zzzz";
            }
        });


        map.print();
        env.execute();
    }
}
