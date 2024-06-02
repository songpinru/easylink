package com.addnewer.easylink.kafka;

import com.addnewer.easylink.utils.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;


import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author pinru
 * @version 1.0
 */
public class KafkaPojoDeserialization<T> implements DeserializationSchema<T> {
    private final Class<T> clazz;

    public KafkaPojoDeserialization(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return JsonUtil.fromBytes(message,clazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return PojoTypeInfo.of(clazz);
    }

}
