package com.atguigu.day03.transform;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_Transform_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapProcess = streamSource.process(new ProcessFunction<String, Tuple2<String, Long>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //1.将数据按照空格切分
                String[] words = value.split(" ");

                //2.将数据转为Tuple
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = flatMapProcess.keyBy(0);
       /* keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Object>() {
        })*/

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();
        env.execute();
    }
}
