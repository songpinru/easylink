package com.addnewer;

import com.addnewer.api.AppSource;
import com.addnewer.api.FlinkJob;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author pinru
 * @version 1.0
 */
public class ExampleJob implements FlinkJob {
    private final AppSource<String> appSource;
    private final SinkFunction<String> sinkFunction;

    public ExampleJob(AppSource<String> appSource, SinkFunction<String> sinkFunction) {
        this.appSource = appSource;
        this.sinkFunction = sinkFunction;
    }

    @Override
    public void run() throws Exception {
        appSource.getDataStream().addSink(sinkFunction);
    }
}
