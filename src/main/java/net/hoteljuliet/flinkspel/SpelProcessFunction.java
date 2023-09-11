package net.hoteljuliet.flinkspel;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hoteljuliet.spel.Context;
import net.hoteljuliet.spel.Pipeline;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpelProcessFunction extends ProcessFunction<Map<String, Object>, Map<String, Object>> implements CheckpointedFunction {

    public static ObjectMapper objectMapper = new ObjectMapper();

    private String name;
    private String config;
    private transient Pipeline pipeline;

    private ListState<String> operatorState;

    private ConcurrentHashMap<String, Object> state;

    public SpelProcessFunction(String name, String config) {
        this.name = name;
        this.config = config;
    }

    public void open(Configuration parameters) throws Exception {
        // if not restored from a checkpoint, create new pipeline
        if (null == pipeline) {
            pipeline = Pipeline.fromString(config);
            pipeline.build();
        }
        if (null == state) {
            state = new ConcurrentHashMap<>();
        }
    }

    public void processElement(Map<String, Object> event, ProcessFunction<Map<String, Object>, Map<String, Object>>.Context context, Collector<Map<String, Object>> collector) throws Exception {

        synchronized (this) {

            /**
             * In processElement() - the SPEl context contains the inbound event, an in-memory state object, and a flag that it's not onTimer().
             * This lets the pipeline do any (or all) of the following:
             * 1) a stateless transform of the event from what was received to an output
             * 2) add enrichment data from state to the event (for example, S3 data tha twas fetched during onTimer())
             * 3) add enrichment data from the pipeline to the event (for example, statistics from the "stats" Statement)
             */
            net.hoteljuliet.spel.Context spelContext = new net.hoteljuliet.spel.Context();
            spelContext.put("_event", event);
            spelContext.put("_state", state);
            spelContext.put("_on_timer", false);
            pipeline.execute(spelContext);

            // if _collect is true, collect _output
            if (spelContext.containsKey("_collect")) {
                Boolean collect = spelContext.getField("_collect");
                if (collect) {
                    collector.collect(spelContext.<Map<String, Object>>getField("_output"));
                }
            }

            // if _clear is true, clear the state
            if (spelContext.containsKey("_clear")) {
                Boolean clear = spelContext.getField("_clear");
                if (clear) {
                    state.clear();
                }
            }

            // if _timer is true, set up the next timer for _timer seconds in the future
            if (spelContext.containsKey("_timer")) {
                Integer timerSeconds = spelContext.getField("_timer_seconds");
                context.timerService().registerProcessingTimeTimer(everyNthSeconds(context.timerService().currentProcessingTime(), timerSeconds));
            }
        }
    }


    public void onTimer(long timestamp, ProcessFunction<Map<String, Object>, Map<String, Object>>.OnTimerContext context, Collector<Map<String, Object>> collector) throws Exception {
        synchronized (this) {

            /**
             * In onTimer - the SPEl context contains the in-memory state object, and a flag that it's being invoked in onTimer().
             * This lets the pipeline perform periodic processing, like fetching enrichment data form S3 (or some other external service)
             */
            net.hoteljuliet.spel.Context spelContext = new net.hoteljuliet.spel.Context();
            spelContext.put("_state", state);
            spelContext.put("_on_timer", true);
            pipeline.execute(spelContext);

            // if _collect is true, collect _output
            if (spelContext.containsKey("_collect")) {
                Boolean collect = spelContext.getField("_collect");
                if (collect) {
                    collector.collect(spelContext.<Map<String, Object>>getField("_output"));
                }
            }

            // if _clear is true, clear the state
            if (spelContext.containsKey("_clear")) {
                Boolean clear = spelContext.getField("_clear");
                if (clear) {
                    state.clear();
                }
            }
        }
    }

    public void close() throws Exception {
        ;
    }

    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        synchronized (this) {
            operatorState.clear();
            String pipelineString = pipeline.snapshot();
            operatorState.add(pipelineString);
            String stateString = objectMapper.writeValueAsString(state);
            operatorState.add(stateString);
        }
    }

    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<String>(name, String.class);
        operatorState = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);

        if (functionInitializationContext.isRestored()) {
            Iterator<String> iterator = operatorState.get().iterator();

            // restore the pipeline
            String pipelineString = iterator.next();
            pipeline = Pipeline.restore(pipelineString);

            // restore the state object
            String stateString = iterator.next();
            state = new ConcurrentHashMap<String, Object>(objectMapper.readValue(stateString, Map.class));
        }
    }

    private long everyNthSeconds(long currentProcessingTime, int seconds) {
        long millis = seconds * 1000;
        return (currentProcessingTime / millis) * millis + millis;
    }
}
