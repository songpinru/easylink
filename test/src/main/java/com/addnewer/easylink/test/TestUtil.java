package com.addnewer.easylink.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

/**
 * @author pinru
 * @version 1.0
 */
public class TestUtil {
    public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> map(MapFunction<IN, OUT> statefulMapFunction) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new StreamMap<>(statefulMapFunction));
    }

    public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> flatMap(FlatMapFunction<IN, OUT> statefulFlatMapFunction) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));
    }

    public static <IN> OneInputStreamOperatorTestHarness<IN, IN> filter(FilterFunction<IN> statefulFilterFunction) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new StreamFilter<>(statefulFilterFunction));
    }

    public static <IN, OUT> OneInputStreamOperatorTestHarness<IN, OUT> process(ProcessFunction<IN, OUT> statefulProcessFunction) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(statefulProcessFunction));
    }



    public static <K, IN, OUT> KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> keyedMap(MapFunction<IN, OUT> statefulMapFunction, KeySelector<IN, K> keySelector) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new StreamMap<>(statefulMapFunction), keySelector, TypeInformation.of(new TypeHint<K>() {}));
    }

    public static <K, IN, OUT> KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> keyedMap(FlatMapFunction<IN, OUT> statefulFlatMapFunction, KeySelector<IN, K> keySelector) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction), keySelector, TypeInformation.of(new TypeHint<K>() {}));
    }

    public static <K,IN> KeyedOneInputStreamOperatorTestHarness<K, IN,IN> keyedFilter(FilterFunction<IN> statefulFilterFunction,KeySelector<IN, K> keySelector) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new StreamFilter<>(statefulFilterFunction),keySelector,TypeInformation.of(new TypeHint<K>() {}));
    }
    public static <K, IN, OUT> KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> keyedProcess(KeyedProcessFunction<K, IN, OUT> statefulKeyedProcessFunction, KeySelector<IN, K> keySelector) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(statefulKeyedProcessFunction), keySelector, TypeInformation.of(new TypeHint<K>() {}));
    }

}
