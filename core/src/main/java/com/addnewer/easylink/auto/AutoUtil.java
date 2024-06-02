package com.addnewer.easylink.auto;

import com.addnewer.easylink.utils.PojoUtil;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author pinru
 * @version 1.0
 */
public class AutoUtil {
    public static <T> T createBean(Class<T> clazz, String name, ParameterTool config) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String prefix = "";
        if (clazz.isAnnotationPresent(Prefix.class)) {
            prefix = clazz.getAnnotation(Prefix.class).value();
            if (!prefix.endsWith(".")) {
                prefix = prefix + ".";
            }
            if (name != null && !name.isEmpty()) {
                prefix = prefix + name + ".";
            }
        }
        T bean = clazz.getConstructor().newInstance();
        for (final Field field : clazz.getDeclaredFields()) {
            if (config.has(prefix + field.getName())) {
                PojoUtil.transform(bean, field, config.get(prefix + field.getName()));
            }
        }
        return bean;
    }
}
