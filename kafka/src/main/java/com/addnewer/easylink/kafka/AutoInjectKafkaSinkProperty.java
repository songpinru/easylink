package com.addnewer.easylink.kafka;

import com.addnewer.easylink.core.AutoInject;
import com.google.auto.service.AutoService;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;

/**
 * @author pinru
 * @version 1.0
 */
@AutoService(AutoService.class)
public class AutoInjectKafkaSinkProperty implements AutoInject<KafkaSinkProperty> {
    @Override
    public boolean isAutoInject(Class<?> clazz) {
        return KafkaSourceProperty.class.equals(clazz);
    }

    @Override
    public KafkaSinkProperty auto(String name, ParameterTool config) {
        try {
            return KafkaUtils.getKafkaSinkProperty(config,name);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
