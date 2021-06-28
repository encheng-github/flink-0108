package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

        //方法一:自定义
//        SingleOutputStreamOperator<Integer> mapDStream = streamSource.map(new MyMap());

        //方法二:匿名实现类
        SingleOutputStreamOperator<Integer> mapDStream = streamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }
        });

        mapDStream.print();
        env.execute();

    }
    //map写法一:通过自定义类去实现MapFunction接口
    public static class MyMap implements MapFunction<Integer,Integer>{
        @Override
        public Integer map(Integer integer) throws Exception {
            return integer*integer;
        }
    }
}
