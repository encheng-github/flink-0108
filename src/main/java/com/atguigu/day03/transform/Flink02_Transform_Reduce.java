package com.atguigu.day03.transform;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2])
                                );
                    }
                }).keyBy(r->r.getId())
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1,
                                              WaterSensor value2) throws Exception {
                        System.out.println("reduce...");
                        return new WaterSensor(value1.getId(),value2.getTs(),
                                Math.max(value1.getVc(),value2.getVc()));
                    }
                })
                .print();
        env.execute();
        /*
        *注意:
        1、 一个分组的第一条数据来的时候，不会进入reduce方法。
        2、 输入和输出的 数据类型，一定要一样。

        *
        * */
    }
}
