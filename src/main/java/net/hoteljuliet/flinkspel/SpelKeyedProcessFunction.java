package net.hoteljuliet.flinkspel;

import net.hoteljuliet.spel.Pipeline;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpelKeyedProcessFunction extends KeyedProcessFunction<String, Map<String, Object>, Map<String, Object>> {

    private String name;
    private Map<String, Object> pipelineConfig;
    private Pipeline pipeline;
    private ConcurrentHashMap<String, Object> state;
    private ValueState<Tuple2<Pipeline, ConcurrentHashMap>> streamState;

    public SpelKeyedProcessFunction(String name, Map<String, Object> pipelineConfig) {
        this.name = name;
        this.pipelineConfig = pipelineConfig;
    }

    @Override
    public void processElement(Map<String, Object> in, KeyedProcessFunction<String, Map<String, Object>, Map<String, Object>>.Context functionContext, Collector<Map<String, Object>> collector) throws Exception {
        try {
            net.hoteljuliet.spel.Context context = new net.hoteljuliet.spel.Context();
            context.addField("_in", in);
            context.addField("_state", state);
            context.addField("_onTimer", false);
            pipeline.execute(context);

            if (context.hasField("_collect")) {
                if (BooleanUtils.isTrue(context.getField("_collect"))) {
                    Map<String, Object> out = context.getField("_out");
                    collector.collect(out);
                }
            }

            if (context.containsKey("_timer")) {
                Integer timerSeconds = context.getField("_timer_seconds");
                functionContext.timerService().registerProcessingTimeTimer(everyNthSeconds(functionContext.timerService().currentProcessingTime(), timerSeconds));
            }

            if (context.hasField("_clear")) {
                if (BooleanUtils.isTrue(context.getField("_clear"))) {
                    state.clear();
                }
            }
        }
        catch (Exception ex) {
            ;
        }
        finally {
            ;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String descriptorName = this.getClass().getName() + name;
        TypeHint typeHint = new TypeHint<Tuple2<Pipeline, ConcurrentHashMap>>(){};
        TypeInformation typeInformation = TypeInformation.of(typeHint);
        pipeline = Pipeline.fromMap(pipelineConfig);
        pipeline.parse();
        state = new ConcurrentHashMap<>();

        // default value, should only be used once
        Tuple2<Pipeline, ConcurrentHashMap> tuple = new Tuple2<>(pipeline, state);
        ValueStateDescriptor<Tuple2<Pipeline, ConcurrentHashMap>> stateDescriptor = new ValueStateDescriptor<Tuple2<Pipeline, ConcurrentHashMap>>(descriptorName, typeInformation, tuple);

        // state will be restored from state backend
        streamState = getRuntimeContext().getState(stateDescriptor);
        pipeline = streamState.value().f0;
        state = streamState.value().f1;
    }

    public void onTimer(long timestamp, ProcessFunction<Map<String, Object>, Map<String, Object>>.OnTimerContext onTimerContext, Collector<Map<String, Object>> collector) throws Exception {
        net.hoteljuliet.spel.Context context = new net.hoteljuliet.spel.Context();
        context.addField("_state", state);
        context.addField("_onTimer", true);
        pipeline.execute(context);

        if (context.hasField("_collect")) {
            if (BooleanUtils.isTrue(context.getField("_collect"))) {
                Map<String, Object> out = context.getField("_out");
                collector.collect(out);
            }
        }

        if (context.hasField("_clear")) {
            if (BooleanUtils.isTrue(context.getField("_clear"))) {
                state.clear();
            }
        }
    }


    private long everyNthSeconds(long currentProcessingTime, int seconds) {
        long millis = seconds * 1000;
        return (currentProcessingTime / millis) * millis + millis;
    }
}
