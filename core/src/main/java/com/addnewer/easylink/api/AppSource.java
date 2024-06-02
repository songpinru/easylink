package com.addnewer.easylink.api;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.lang.reflect.InvocationTargetException;

/**
 * @author pinru
 * @version 1.0
 */
public interface AppSource<T> {
    DataStream<T> getDataStream() throws InvocationTargetException, IllegalAccessException;
}
