package com.yindahu;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class checkpoint implements Serializable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //事件流
        DataStream<Tuple2> dataStream1 = env.fromElements(Tuple2.of(1L, 1L), Tuple2.of(1L, 11L), Tuple2.of(2L, 1L), Tuple2.of(2L, 11L), Tuple2.of(3L, 1L),Tuple2.of(3L, 11L));
        env.enableCheckpointing(10000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        dataStream1.keyBy((a)->a.f0).sum(1).print();
        try {
            env.execute("checkpoint_flink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
