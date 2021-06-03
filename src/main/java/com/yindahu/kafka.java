package com.yindahu;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

public class kafka {
    public static void main(String[] args) {
        DeserializationSchema<String> schema=new DeserializationSchema<String>() {
            @Override
            public String deserialize(byte[] message) throws IOException {
                return new String(message,0,message.length,"utf-8");
            }

            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(String.class);
            }
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        properties.setProperty("group.id", "flink");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer("FlinkTopic", new SimpleStringSchema(), properties);
        kafkaSource.setStartFromGroupOffsets();

        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaSource);
        stringDataStreamSource.print();
        try {
            env.execute("flink_kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
