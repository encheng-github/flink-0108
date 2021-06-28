package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1, 2, 3, 4, 5)
//        env.socketTextStream("hadoop102",9999)
//        env.readTextFile("input")
                .map(new MyRichMap())
                .print();
        env.execute();
    }
    public static class MyRichMap extends RichMapFunction<Integer,Integer>{
        //默认声明周期,open在每个并行度,先执行一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public Integer map(Integer integer) throws Exception {
            System.out.println(getRuntimeContext().getTaskName());
            return integer*integer;
        }

        //默认声明周期,close在每个并行度结束,最后执行一次,文件是两次,为了读取文件夹
        @Override
        public void close() throws Exception {
            System.out.println("close");
        }

        //运行时,上下文环境
        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }
    }
}
