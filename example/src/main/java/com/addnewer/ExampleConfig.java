package com.addnewer;

import com.addnewer.api.AppSource;
import com.addnewer.api.Bean;
import com.addnewer.api.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author pinru
 * @version 1.0
 */
@Configuration
public class ExampleConfig {

    @Bean
    public AppSource<String> bean(StreamExecutionEnvironment env) {
        return ()->env.fromElements("ss","aa","bb","cc");
    }

    @Bean
    public SinkFunction<String> sink(){
        return new PrintSinkFunction<>();
    }
}
