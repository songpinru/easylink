package com.addnewer.easylink.core;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Set;

/**
 * @author pinru
 * @version 1.0
 */
public interface AutoInject<T> {

    boolean isAutoInject(Class<?> clazz);

    T auto(String name, ParameterTool config);

}
