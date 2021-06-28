package com.atguigu.view;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class Flink10_JdbcSink {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //3.将数据写入到Mysql
        streamOperator.addSink(JdbcSink.sink("insert into sensor values" +
                "(?,?,?)", new JdbcStatementBuilder<WaterSensor>() {
            @Override
            public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                preparedStatement.setObject(1, waterSensor.getId());
                preparedStatement.setObject(2, waterSensor.getTs());
                preparedStatement.setObject(3, waterSensor.getVc());
            }
        },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL" +
                                "=false")
                        .withUsername("root")
                        .withPassword("123456")
                .build()
                ));

        //4.启动
        env.execute();
    }
}
