package com.atguigu.day05;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink05_WaterMark_EventSession_Custom {
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

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriodic(2 * 1000);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                return waterSensor.getTs() * 1000;
            }
        }));


        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        //??????????????????????????????????????????
        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "??????key: " + key
                        + "??????: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") ????????? "
                        + elements.spliterator().estimateSize() + "????????? ";
                out.collect(msg);
            }
        }).print();
        env.execute();
    }

    public static class MyPeriodic implements WatermarkGenerator<WaterSensor>{

        //???????????????
        private long maxTimestamp;

        //????????????
        private long outOfOrdernessMillis;

        public MyPeriodic(long outOfOrdernessMillis) {
            this.outOfOrdernessMillis = outOfOrdernessMillis;
            this.maxTimestamp=Long.MIN_VALUE+outOfOrdernessMillis+1;
        }

        //???????????????
        @Override
        public void onEvent(WaterSensor waterSensor, long l, WatermarkOutput watermarkOutput) {
            System.out.println("??????waterMark");
            maxTimestamp = Math.max(this.maxTimestamp, l);

        }

        //???????????????
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//            System.out.println("??????waterMark");
            watermarkOutput.emitWatermark(new Watermark(maxTimestamp-outOfOrdernessMillis-1));
        }
    }
}
