package com.yindahu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.util.Map;

public class ttl extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum;
        if(sum.value() !=null)
        currentSum = sum.value();
        else
            currentSum=Tuple2.of(0L,0L);

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);


        out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>("average",Types.TUPLE(Types.LONG, Types.LONG));
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                .build();

        descriptor.enableTimeToLive(ttlConfig);

        sum = getRuntimeContext().getState(descriptor);

    }


    public static void main(String[] args) {
        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream1 =  env
                .socketTextStream("master", 9999);
        DataStream<Tuple2<Long,Long>> dataStream2 =dataStream1.map( (MapFunction<String, Tuple2<Long, Long>>) (value )-> new Tuple2(Long.valueOf(value.split(",")[0]),Long.valueOf(value.split(",")[1]))).returns(Types.TUPLE(Types.LONG,Types.LONG));
                dataStream2.keyBy(value -> value.f0)
                .flatMap(new ttl())
                .print();
        try {
            env.execute("state_ttl_flink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}