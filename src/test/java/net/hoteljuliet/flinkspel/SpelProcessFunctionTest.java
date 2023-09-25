package net.hoteljuliet.flinkspel;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hoteljuliet.spel.Parser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SpelProcessFunctionTest {

    @Test
    public void test() throws Exception {

        String config =
                "stopOnFailure: true\n" +
                "config:\n" +
                "  - crunch: { dest: _out.someTotal, exp: \"{{_in.total1}} + {{_in.total2}}\" }\n" +
                "  - add: { dest: _out.name, value: \"{{_in.name}}\" }\n" +
                "  - add: { dest: _collect, value: true }\n";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Map<String, Object> event = new HashMap<>();
        event.put("name", "Dennis");
        event.put("age", "35");
        event.put("total1", "12000");
        event.put("total2", "1200");
        event.put("total3", "120");
        event.put("total4", "12");
        event.put("lastPurchaseUnixMs", "1694464850980");
        DataStream<Map<String, Object>> in = env.fromElements(event);

        Map<String, Object> pipelineConfig = Parser.objectMapper.readValue(config, Map.class);
        DataStream<Map<String, Object>> out = in.process(new SpelProcessFunction("test", pipelineConfig));
        out.sinkTo(new PrintSink<>("printSink-> "));
        env.execute();
    }

    @Test
    public void test_snapshotAndRecovery() throws Exception {
        // TODO:
    }
}
