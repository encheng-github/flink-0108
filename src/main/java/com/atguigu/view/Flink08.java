package com.atguigu.view;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;



public class Flink08 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.读取端口数据
        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String s, long l) {
                        String[] split = s.split(",");
                        return Long.parseLong(split[1]) * 1000L;
                    }
                }));

        //3.将每行数据转为JavaBean对象,提取时间戳生成watermark
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
              /*  .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000L;
            }
        }));
*/

        //4.按照传感器Id分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream =
                waterSensorDS.map(new MapFunction<WaterSensor, Tuple2<String,Integer>>() {
                                      @Override
                                      public Tuple2<String, Integer> map(WaterSensor waterSensor) throws Exception {
                                          return new Tuple2<>(waterSensor.getId(),1);
                                      }
                                  }
                ).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        //5.开窗
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>(
                "Slide") {
        };
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30),
                Time.seconds(5))).allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag);

        //6.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0,value1.f1+value2.f1);
//                value1.f1 += value2.f1;
//                return value1;
            }
        });

        //7.打印
        result.print("main>>>>>>>>>>>>>>>>");
        result.getSideOutput(outputTag).print("Slide>>>>>>>>>>>");

        //8.开启
        env.execute();
    }
}
