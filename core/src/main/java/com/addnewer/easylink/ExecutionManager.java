package com.addnewer.easylink;

import org.apache.flink.api.common.ExecutionConfig;

public interface ExecutionManager{
    void manage(ExecutionConfig env);
}
