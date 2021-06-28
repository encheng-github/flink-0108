package com.atguigu.day03.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink09_WordCount_RunTimeMode {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // TODO: 将运行模式改为批处理
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word" +
                ".txt");

        //3.先将一行数据按照空格切分,切成一个一个单词,并且把这些单词组成tuple元组(word,1)
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value,
                                Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");

                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        //4.按照key分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOne.keyBy(0);

        //5.累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        //6.打印
        sum.print();

        //7.开启任务
        env.execute();

    }
}
