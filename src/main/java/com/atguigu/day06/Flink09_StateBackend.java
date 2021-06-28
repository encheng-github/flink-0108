package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink09_StateBackend {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //内存 -->Jobmanager
        env.setStateBackend(new MemoryStateBackend());

        //文件系统
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink" +
                "/ck"));

        //RocksDB
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020" +
                "/flink/RocksDB"));

        env.getCheckpointConfig().enableUnalignedCheckpoints();
    }
}
