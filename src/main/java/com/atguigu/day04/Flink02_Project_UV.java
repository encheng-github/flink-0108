package com.atguigu.day04;

import com.atguigu.day03.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取文件数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为javaBean,然后过滤pv行为的数据,并且将数据组成Tuple元组返回
        SingleOutputStreamOperator<Tuple2<String, Long>> uvToUserIdDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                //a.将数据转为JavaBean
                String[] split = s.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
                //b.首先过滤出pv行为的数据
                if ("pv".equals(userBehavior.getBehavior())) {
                    //c.将数据转为Tuple返回
                    collector.collect(Tuple2.of("uv", userBehavior.getUserId()));
                }
            }
        });

        //4.将相同的key数据聚合到同一并行度中
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uvToUserIdDStream.keyBy(0);

        //5.对数据去重,并计算去重后的个数
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Long>() {
            HashSet<Long> uids = new HashSet<>();
            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                //对数据做去重,->set集合
                uids.add(value.f1);
                out.collect((long)uids.size());
            }


        }).print();

        //8.打印数据
//        result.print();
        env.execute();
    }
}
