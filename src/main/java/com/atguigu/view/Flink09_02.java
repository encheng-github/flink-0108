package com.atguigu.view;

import com.atguigu.day02.bean.WaterSensor;
import com.atguigu.view.bean.IdCount;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/*
 * 接上一题,统计完WordCount之后统计每个窗内部Count TopN
 *   1.计算在每个并行度下的topN
 *   2.需要重分区(keyby(添加一个时间戳,窗口信息(窗口的开始时间)),global也行,效率低)
 *   3.把一个窗口的所有数据收集到一起(状态(历史)编程--list)
 *   4.怎么确定式该窗口的最后一条数据,然后计算  -->在窗口计算的基础上+100ms  即定时器(processFunction利用状态编程+定时器)
 *   5.断开的数据取出来排序
 *
 *
 * */

public class Flink09_02 {
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
        waterSensorDS.print("waterSensorDS>>>>>>>>>>");
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
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30),
                Time.seconds(5))).allowedLateness(Time.seconds(5));

        //6.聚合,计算wordCount并添加窗口的信息
        SingleOutputStreamOperator<IdCount> aggregateDS =
                windowedStream.aggregate(new AggFunc(), new IdCountWindowFunc());
        aggregateDS.print("aggregateDS>>>>>>>>>>");

        //7.按照窗口信息重新分组
        KeyedStream<IdCount, Long> tsKeyedStream =
                aggregateDS.keyBy(IdCount::getTs);

        //8.processFunction利用状态编程+定时器 ,完成最终的排序任务
        SingleOutputStreamOperator<String> result = tsKeyedStream.process(new TopNCountFunc(3));

        //9.打印结果
        result.print();

        //10.开启
        env.execute();
    }

    public static class AggFunc implements AggregateFunction<Tuple2<String,
            Integer>,Integer,Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
            return integer+1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }


    public static class IdCountWindowFunc implements WindowFunction<Integer,
            IdCount,String,TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window,
                          Iterable<Integer> input, Collector<IdCount> out) throws Exception {
            //取出Count数据
            Integer ct = input.iterator().next();

            //取出窗口信息
            long windowEnd = window.getEnd();

            //写出数据
            out.collect(new IdCount(windowEnd,key,ct));

        }
    }

    public static class TopNCountFunc extends KeyedProcessFunction<Long,
            IdCount,String> {
        //定义属性
        private Integer topSize;

        public TopNCountFunc(Integer topSize){
            this.topSize=topSize;
        }

        //定义状态
        private MapState<String,IdCount> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState=
                    getRuntimeContext().getMapState(new MapStateDescriptor<String, IdCount>("map-state",String.class,IdCount.class));
        }

        @Override
        public void processElement(IdCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据加入状态
            mapState.put(value.getId(),value);

            //注册定时器,用于触发当数据收集完成时,排序输出
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(value.getTs()+100L);

            //注册定时器,用于触发状态清除
            timerService.registerEventTimeTimer(value.getTs()+5000L+100L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if (timestamp==ctx.getCurrentKey()+5000L+100L) {
                //清空状态
                mapState.clear();
                return;
            }

            //取出状态中的数据
            Iterator<IdCount> iterator = mapState.values().iterator();
            ArrayList<IdCount> idCounts = Lists.newArrayList(iterator);

            //排序
            idCounts.sort(new Comparator<IdCount>() {
                @Override
                public int compare(IdCount o1, IdCount o2) {
                    if (o1.getCt()>o2.getCt()) {
                        return -1;
                    }else if (o1.getCt()<o2.getCt()){
                        return 1;
                    }else{
                        return 0;
                    }

                }
            });

            //输出
            StringBuilder sb = new StringBuilder("=====================\n");
            for (int i = 0; i < Math.min(idCounts.size(),topSize); i++) {

                //输出数据
                IdCount idCount = idCounts.get(i);
                sb.append("TOP").append(i+1)
                        .append(",ID:").append(idCount.getId())
                        .append(",CT:").append(idCount.getCt())
                        .append("\n");
            }
            sb.append("=====================\n");
            out.collect(sb.toString());
//清空状态
//            listState.clear();

        }
    }
}
