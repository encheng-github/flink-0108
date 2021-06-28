package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);

        //2.读取数据(有界)
        DataStreamSource<String> streamSource = env.readTextFile("input");

        //3.先将一行数据按照空格切分,切成一个一个单词,并且把这些单词组成tuple元组(word,1)
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value,
                                Collector<Tuple2<String, Long>> out) throws Exception {
                //按照空格切分单词
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        //4.将相同key的数据聚合到一块
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOne.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });
        //5.累加
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        //6.打印
        sum.print();

        env.execute();
    }
}
