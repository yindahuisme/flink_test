package com.yindahu;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;


public class test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<String> objectWatermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1));

        DataStream<String> dataStream1 =  env
                .socketTextStream("master", 9999)
                .assignTimestampsAndWatermarks(objectWatermarkStrategy);

        DataStream<Tuple2<String,Integer>> dataStream=dataStream1
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        try {
            env.execute("Window WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(",")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
    public static class my_watermark_event_time_asigner implements WatermarkStrategy {

        public void  my_watermark_event_time_asigner(){
            this.withIdleness(Duration.ofSeconds(100));
        }
        @Override
        public TimestampAssigner<String> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            return (message,time)-> {
                try {
                    return Math.max(ft.parse(message.split(",")[0]).getTime(),time);
                } catch (ParseException e) {
                    e.printStackTrace();
                    return time;
                }
            };
        }

        @Override
        public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return  new mywater_mark_generator();
        }

        public static class mywater_mark_generator implements WatermarkGenerator<String>{

            @Override
            public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
                if(event.split(",")[0].endsWith("00"))
                output.emitWatermark(new Watermark(eventTimestamp-10000));
                else
                output.markIdle();
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }
        }
    }
    }


