package com.addnewer.api;

/**
 * Flink 任务的主要逻辑,用于实现解耦
 * @author pinru
 * @version 1.0
 */
public interface FlinkJob {
    void run() throws Exception;
}
