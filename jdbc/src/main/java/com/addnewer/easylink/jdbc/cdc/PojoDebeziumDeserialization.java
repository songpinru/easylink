package com.addnewer.easylink.jdbc.cdc;

import com.addnewer.easylink.utils.JsonUtil;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;


/**
 * @author pinru
 * @version 1.0
 */
public class PojoDebeziumDeserialization<T> implements DebeziumDeserializationSchema<Tuple2<String, T>> {
    private final Class<T> clazz;


    public PojoDebeziumDeserialization(Class<T> clazz) {
        this.clazz = clazz;
    }


    @Override
    public TypeInformation<Tuple2<String, T>> getProducedType() {
        return TUPLE(STRING, POJO(clazz));
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Tuple2<String, T>> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            T t = getInstance(value);
            out.collect(Tuple2.of("insert", t));
        } else if (op == Envelope.Operation.UPDATE) {
            T t = getInstance(value);
            out.collect(Tuple2.of("update", t));
        }
    }

    private T getInstance(Struct value) throws JsonProcessingException {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        HashMap<String, Object> map = convertStruct(after);
        String json = JsonUtil.toJson(map);
        T t = JsonUtil.fromJson(json, clazz);
        return t;
    }

    private static HashMap<String, Object> convertStruct(Struct after) {
        HashMap<String, Object> map = new HashMap<>();
        after.schema().fields().forEach(field -> {
            Object value;
            if (STRUCT == field.schema().type()) {
                value = convertStruct(after.getStruct(field.name()));
            } else {
                value = after.get(field.name());
            }
            map.put(field.name(), value);
        });
        return map;
    }
}
