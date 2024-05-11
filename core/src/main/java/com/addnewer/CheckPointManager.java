package com.addnewer;

import org.apache.flink.streaming.api.environment.CheckpointConfig;

public interface CheckPointManager {
    void manage(CheckpointConfig checkpointConfig);
}
