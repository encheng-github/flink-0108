package com.atguigu.day04.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink12_Window_Function_Agg {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

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

        //开启滚动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // TODO: 窗口函数 aggregate
        windowedStream.aggregate(new AggregateFunction<Tuple2<String, Long>,Long, Long>() {
            //创建累加器-->初始化一个累加器
            @Override
            public Long createAccumulator() {
                System.out.println("初始化累加器...");
                return 0L;
            }

            //累加器做累加操作
            @Override
            public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
                System.out.println("累加器操作");
                return aLong+1L;
            }

            //返回结果
            @Override
            public Long getResult(Long aLong) {
                System.out.println("结果数据");
                return aLong;
            }

            //累加器的合并,只有会话窗口才会调用
            @Override
            public Long merge(Long aLong, Long acc1) {
                System.out.println("累加器的合并");
                return aLong+acc1;
            }
        }).print();

        //7.开启任务
        env.execute();

    }
}
