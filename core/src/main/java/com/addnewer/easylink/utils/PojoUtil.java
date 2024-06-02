package com.addnewer.easylink.utils;


import org.apache.flink.calcite.shaded.com.google.common.base.CaseFormat;
import org.apache.flink.calcite.shaded.com.google.common.base.Converter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author pinru
 * @version 1.0
 */
public class PojoUtil {
    private static final Converter<String, String> CAMEL_SNAKE = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE);

    public static void transform(Object pojo, Field field, String value) throws IllegalAccessException {
        field.setAccessible(true);
        try {
            field.set(pojo, JsonUtil.fromJson(value, field.getType()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("不支持的格式!", e);
        }
    }





    public static String camelToSnake(String name) {
        return CAMEL_SNAKE.convert(name);
    }
}
