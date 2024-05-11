package com.addnewer.api;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author pinru
 * @version 1.0
 */
public interface AppSource<T> {
    DataStream<T> getDataStream();
}
