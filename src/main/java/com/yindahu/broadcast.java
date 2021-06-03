package com.yindahu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class broadcast {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //事件流
        DataStream<String> dataStream1 =  env
                .socketTextStream("master", 9999);
        //广播流
        DataStream<String> dataStream2 =  env
                .socketTextStream("master", 9998);

        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {}));
        BroadcastStream<String> broadcast_string = dataStream2.broadcast(ruleStateDescriptor);

        //连接
        DataStream<Integer> dataStream3=dataStream1.connect(broadcast_string).process(new my_broad_cast_process());
        dataStream3.print();

        try {
            env.execute("broadcast_flink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class my_broad_cast_process extends BroadcastProcessFunction<String, String, Integer> {
        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
        "RulesBroadcastState",
        BasicTypeInfo.STRING_TYPE_INFO,
        TypeInformation.of(new TypeHint<String>() {
        }));

@Override
public void processElement(String value, ReadOnlyContext ctx, Collector<Integer> out) throws Exception {
        String status = ctx.getBroadcastState(ruleStateDescriptor).get("status");
        if (value.equals(status))
        out.collect(1);
        else
        out.collect(0);
        }

@Override
public void processBroadcastElement(String value, Context ctx, Collector<Integer> out) throws Exception {
        ctx.getBroadcastState(ruleStateDescriptor).put("status", value);
        }
        }