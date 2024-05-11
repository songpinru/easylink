package com.addnewer;

import com.addnewer.api.Bean;
import com.addnewer.api.Configuration;
import com.addnewer.api.Value;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author pinru
 * @version 1.0
 */
@Configuration
public class DefaultConfiguration {

    @Bean
    public StreamExecutionEnvironment env(org.apache.flink.configuration.Configuration config, CheckPointManager checkpointManager, ExecutionManager executionManager) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        checkpointManager.manage(env.getCheckpointConfig());
        executionManager.manage(env.getConfig());
        return env;
    }

    @Bean
    public CheckPointManager checkpointManager() {
        return checkpointConfig -> {
            checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            checkpointConfig.setCheckpointInterval(5000L);
        };
    }

    @Bean
    public ExecutionManager executionManager() {
        return executionConfig -> {
        };
    }

    @Bean
    public org.apache.flink.configuration.Configuration config() {
        return new org.apache.flink.configuration.Configuration();
    }

}


