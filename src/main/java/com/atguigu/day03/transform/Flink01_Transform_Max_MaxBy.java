package com.atguigu.day03.transform;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_Transform_Max_MaxBy {
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
//                .max("vc")
                .maxBy("vc",true)
                .print();
        env.execute();
        /*
        * 注意:
	滚动聚合算子： 来一条，聚合一条
        1、聚合算子在 keyby之后调用，因为这些算子都是属于 KeyedStream里的
        2、聚合算子，作用范围，都是分组内。 也就是说，不同分组，要分开算。
        3、max、maxBy的区别：
            max：取指定字段的当前的最大值，如果有多个字段，其他非比较字段，以第一条为准
            maxBy：取指定字段的当前的最大值，如果有多个字段，其他字段以最大值那条数据为准；
            如果出现两条数据都是最大值，由第二个参数决定： true => 其他字段取 比较早的值； false => 其他字段，取最新的值

        * */
    }
}
