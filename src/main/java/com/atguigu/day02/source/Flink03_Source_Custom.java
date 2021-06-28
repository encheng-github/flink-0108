package com.atguigu.day02.source;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink03_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());

        streamSource.print();

        env.execute();
    }


    public static class MySource implements SourceFunction<WaterSensor>{

        private Random random=new Random();
        private Boolean running=true;
        /**
         * 用来发送数据
         *
         * @param sourceContext
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {

            while (running) {
                sourceContext.collect(new WaterSensor("sensor"+random.nextInt(5),System.currentTimeMillis(),random.nextInt(10)*100));
                Thread.sleep(1000);
            }
        }

        /**
         * 取消任务的,系统内部调用
         */
        @Override
        public void cancel() {

            running=false;
        }
    }
}
