package com.atguigu.day03.app;

import com.atguigu.day03.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink10_Project_PV {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取文件数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为javaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream =
                streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //4.过滤出PV行为的数据
        SingleOutputStreamOperator<UserBehavior> pvDStream = userBehaviorDStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return "pv".equals(userBehavior.getBehavior());
            }
        });

        //5.将数据转为Tuple(pv,1L)
        SingleOutputStreamOperator<Tuple2<String, Long>> pvToOneDStream =
                pvDStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                return Tuple2.of("pv", 1L);
            }
        });

        //6.将相同key的数据聚合到一起,为了使用聚合算子
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream =
                pvToOneDStream.keyBy(0);

        //7.累加求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        //8.打印数据
        result.print();
        env.execute();
    }
}
