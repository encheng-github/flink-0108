package com.atguigu.day06;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink07_OnTimer_Exe_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //2.将数据转为javaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });

        //3.keybe
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());

        //4.使用ProcessFunction实现5秒种水位不下降，则报警，且将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
/*            //用来保存上一条的vc值
            private Integer lastVc = Integer.MIN_VALUE;
            //初始化定时器的值
            private Long timer = Long.MIN_VALUE;*/

            //TODO 定义用来保存vc值的状态
            private ValueState<Integer> lastVc;

            //TODO 定义用来保存定时器的时间的状态
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                lastVc=
                        getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state",Integer.class,Integer.MIN_VALUE));
                timer=
                        getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state",Long.class,Long.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                //首先判断当前的vc是否大于上一个vc
                if (value.getVc() > lastVc.value()) {
                    //判断是否有定时器被注册
                    if (timer.value() == Long.MIN_VALUE) {
                        //注册定时器
                        System.out.println("注册定时器");
                        //变更定时器的值,证明已经注册过定时器
                        timer.update(ctx.timerService().currentProcessingTime() + 10000);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    //水位没有上升
                    //删除定时器
                    System.out.println("删除定时器");
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());
                    //将定时器的值恢复
                    timer.clear();
                }
                //更新lastvc
                lastVc.update(value.getVc());
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //报警了!!!
                System.out.println("报警了!!!" + ctx.getCurrentKey());
                //将定时器的值恢复
                timer.clear();
                ctx.output(new OutputTag<String>("报警信息: "){},
                        ctx.getCurrentKey() + "连续5s内上升");
            }
        });

        result.print();
        result.getSideOutput(new OutputTag<String>("报警信息: "){}).print("报警: ");

        env.execute();
    }
}
