package com.atguigu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream(
                "hadoop102", 9999).setParallelism(1);

        //3.先将一行数据按照空格切分,切成一个一个单词,并且把这些单词组成tuple元组(word,1)
        SingleOutputStreamOperator<String> wordFlatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOne = wordFlatMap.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        }).slotSharingGroup("group1");
        SingleOutputStreamOperator<Tuple2<String, Long>> filter = wordToOne.filter(new FilterFunction<Tuple2<String, Long>>() {
            @Override
            public boolean filter(Tuple2<String, Long> value) throws Exception {
                return value.f0.equals("hello");
            }
        });

        //4.按照key分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = filter.keyBy(0);

        //5.累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sum =
                keyedStream.sum(1);

        //6.打印
        sum.print();

        //7.开启任务
        env.execute();

    }
}
