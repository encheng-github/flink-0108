package com.atguigu.view;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;


public class Flink07 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //2.获取端口数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.拆分单词
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        });

        //4.过滤
        SingleOutputStreamOperator<String> result = flatMap
                .keyBy(word->word)
                .filter(new FilterFunction<String>() {
            HashSet<String> hashSet = new HashSet<>();

            @Override
            public boolean filter(String s) throws Exception {
                //如果数据存在添加,否则不添加
                boolean add = hashSet.add(s);
                return add;
            }
        });

        //5.打印
        result.print();

        //6.启动
        env.execute();
    }
}
