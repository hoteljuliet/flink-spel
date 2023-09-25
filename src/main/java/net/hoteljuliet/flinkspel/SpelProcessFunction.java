package net.hoteljuliet.flinkspel;

import net.hoteljuliet.spel.Pipeline;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpelProcessFunction extends ProcessFunction<Map<String, Object>, Map<String, Object>> implements CheckpointedFunction {

    private String name;
    Map<String, Object> pipelineConfig;
    private Pipeline pipeline;
    private ConcurrentHashMap<String, Object> state;
    private transient ListState<Tuple2<Pipeline, ConcurrentHashMap>> checkpointedState;
    private List<Tuple2<Pipeline, ConcurrentHashMap>> checkpoint;

    public SpelProcessFunction(String name, Map<String, Object> pipelineConfig) {
        this.name = name;
        this.pipelineConfig = pipelineConfig;
        this.checkpoint = new ArrayList<>();
        this.state = new ConcurrentHashMap<>();
    }

    @Override
    public void processElement(Map<String, Object> in, ProcessFunction<Map<String, Object>, Map<String, Object>>.Context functionContext, Collector<Map<String, Object>> collector) throws Exception {
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

    @Override
    public void open(Configuration parameters) throws Exception {
        ;
    }

    @Override
    public void close() throws Exception {
        ;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.update(checkpoint);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        String descriptorName = this.getClass().getName() + name;
        TypeHint typeHint = new TypeHint<Tuple2<Pipeline, ConcurrentHashMap>>(){};
        TypeInformation typeInformation = TypeInformation.of(typeHint);
        ListStateDescriptor descriptor = new ListStateDescriptor<>(descriptorName, typeInformation);
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            Tuple2<Pipeline, ConcurrentHashMap> tuple = checkpointedState.get().iterator().next();
            checkpoint.add(tuple);
            pipeline = tuple.getField(0);
            state = tuple.getField(1);
        }
        else {
            pipeline = Pipeline.fromMap(pipelineConfig);
            pipeline.parse();
            Tuple2<Pipeline, ConcurrentHashMap> tuple = new Tuple2<>(pipeline, state);
            checkpoint.add(tuple);
        }
    }

    private long everyNthSeconds(long currentProcessingTime, int seconds) {
        long millis = seconds * 1000;
        return (currentProcessingTime / millis) * millis + millis;
    }
}
