package com.atguigu.day05;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink10_WaterMark_OnTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });


        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(WaterSensor::getId);

        //判断vc大于5的数据将其放入侧输出流中
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                // TODO: 注册基于处理时间的定时器
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+200);
            }

            // TODO: 定时器时间到了之后需要做的事情
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("到时间了...");
                System.out.println(ctx.timerService().currentProcessingTime());
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+200);
            }
        });


        env.execute();
    }
}
