package com.addnewer;

import com.addnewer.easylink.api.AppSource;
import com.addnewer.easylink.api.FlinkJob;
import com.addnewer.easylink.api.Value;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author pinru
 * @version 1.0
 */
public class ExampleJob implements FlinkJob {
    private final AppSource<String> appSource;
    private final SinkFunction<String> sinkFunction;

    @Value("ss")
    Integer aa;
    public ExampleJob(AppSource<String> appSource, SinkFunction<String> sinkFunction) {
        this.appSource = appSource;
        this.sinkFunction = sinkFunction;
    }

    @Override
    public void run() throws Exception {
        System.out.println(aa);
        // appSource.getDataStream().addSink(sinkFunction);
    }
}
