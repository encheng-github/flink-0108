package com.atguigu.day04;

import com.atguigu.day03.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.获取文件数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        streamSource.process(new ProcessFunction<String, Long>() {
            Long count=0L;
            @Override
            public void processElement(String value, Context ctx, Collector<Long> out) throws Exception {
                //取出原始数据中标识行为的数据
                String[] split = value.split(",");
                //判断获取到的行为受否为PV行为
                if ("pv".equals(split[3])) {
                    count++;
                    out.collect(count);
                }
            }
        }).print().setParallelism(1);

        //8.打印数据
        env.execute();
    }
}
