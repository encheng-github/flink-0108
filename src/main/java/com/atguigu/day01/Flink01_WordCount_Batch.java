package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //3.先将一行数据按照空格切分,切成一个一个单词,并且把这些单词组成tuple元组(word,1)
        //flatMap
//        MyFlatMap myFlatMap = new MyFlatMap();
        FlatMapOperator<String, Tuple2<String, Long>> flatMap = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value,
                                Collector<Tuple2<String, Long>> out) throws Exception {
                //将读过来的一行一行数据按照空格切分,切成一个一个单词
                String[] words = value.split(" ");
                for (String word : words) {
//                Tuple2.of(word,1L);
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.将相同key的数据聚合到一起
        UnsortedGrouping<Tuple2<String, Long>> groupBy = flatMap.groupBy(0);

        //5.累加
        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);

        //6.打印
        sum.print();

    }
    //方法一:
//    public static class MyFlatMap implements FlatMapFunction<String,
//            Tuple2<String,Long>>{
//
//        @Override
//        public void flatMap(String value,
//                            Collector<Tuple2<String, Long>> out) throws Exception {
//            //将读过来的一行一行数据按照空格切分,切成一个一个单词
//            String[] words = value.split(" ");
//            for (String word : words) {
////                Tuple2.of(word,1L);
//                out.collect(Tuple2.of(word,1L));
//            }
//        }
//    }
}
