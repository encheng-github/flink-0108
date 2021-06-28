package com.atguigu.day04;

import com.atguigu.day04.bean.AdsClickLog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink04_Project_AdClick {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
        //2.获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //3.处理数据-->将数据组成Tuple元组
        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
            @Override
            public void flatMap(String value,
                                Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );

                out.collect(Tuple2.of(adsClickLog.getProvince()+"_"+adsClickLog.getAdId(),1L));
            }
        }).keyBy(0)//将数据进行聚合
        .sum(1)//5.数据进行累加计算
        .print();

        env.execute();
    }
}
