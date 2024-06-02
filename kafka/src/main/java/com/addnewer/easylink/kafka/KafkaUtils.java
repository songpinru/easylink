package com.addnewer.easylink.kafka;

import com.addnewer.easylink.auto.AutoUtil;
import com.addnewer.easylink.utils.JsonUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

/**
 * @author pinru
 * @version 1.0
 */
public class KafkaUtils {
    public static KafkaSourceProperty getKafkaSourceProperty(ParameterTool config, String name) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return AutoUtil.createBean(KafkaSourceProperty.class, name, config);
    }
    public static KafkaSinkProperty getKafkaSinkProperty(ParameterTool config, String name) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return AutoUtil.createBean(KafkaSinkProperty.class, name, config);
    }

    public static <T> KafkaSource<T> kafkaSourcePojoFromJson(KafkaSourceProperty property, Class<T> clazz) {
        return KafkaSource
                .<T>builder()
                .setBootstrapServers(property.bootstrapServers)
                .setTopics(property.topics)
                .setGroupId(property.groupId)
                .setValueOnlyDeserializer(new KafkaPojoDeserialization<>(clazz))
                .build();
    }

    public static KafkaSource<String> kafkaSourceString(KafkaSourceProperty property) {
        return KafkaSource
                .<String>builder()
                .setBootstrapServers(property.bootstrapServers)
                .setTopics(property.topics)
                .setGroupId(property.groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static KafkaSink<String> kafkaSinkString(KafkaSinkProperty property) {
        KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
                .setRecordSerializer((KafkaRecordSerializationSchema<String>) (s, kafkaSinkContext, aLong) -> new ProducerRecord<>(property.getTopic(), s.getBytes(StandardCharsets.UTF_8)));
        return getSink(property, builder);
    }

    private static<T> KafkaSink<T> getSink(KafkaSinkProperty property, KafkaSinkBuilder<T> builder) {
        builder.setBootstrapServers(property.bootstrapServers);
        if (property.getAcks() != null) {
            builder.setProperty(ProducerConfig.ACKS_CONFIG, property.getAcks());
        }
        if (property.getRetries()!=null){
            builder.setProperty(ProducerConfig.RETRIES_CONFIG, property.getRetries());
        }
        if(property.getBatchSize()!=null){
            builder.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, property.getBatchSize());
        }
        return builder
                .build();
    }

    public static<T> KafkaSink<T> kafkaSinkPojoToJson(KafkaSinkProperty property) {
        KafkaSinkBuilder<T> build = KafkaSink.<T>builder()
                .setRecordSerializer((KafkaRecordSerializationSchema<T>) (t, kafkaSinkContext, aLong) -> {
                    try {
                        return new ProducerRecord<>(property.getTopic(), JsonUtil.toBytes(t));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                });
        return getSink(property,build);

    }
}
