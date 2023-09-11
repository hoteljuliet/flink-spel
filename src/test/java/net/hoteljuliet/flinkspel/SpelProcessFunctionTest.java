package net.hoteljuliet.flinkspel;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.junit.Test;
import org.testcontainers.shaded.org.bouncycastle.est.Source;

import java.util.HashMap;
import java.util.Map;

public class SpelProcessFunctionTest {


    @Test
    public void test() throws Exception {

        //   - crunch: {dest: "mathResult_crunch", exp: "$1 + $2", variables: ["a.a", "z.z"]}
        // add-m: {dest: "copy", exp: "{{a.a}}"}
        String config =
                "stopOnFailure: true\n" +
                "logStackTrace: true\n" +
                "logPerformance: true\n" +
                "config:\n" +
                "  - crunch: { dest: _output.someTotal, exp: $1 + $2, variables: [_event.total1, _event.total2] }\n" +
                "  - add-m: { dest: _output.name, exp: \"{{_event.name}}\" }\n" +
                "  - add-i: { dest: _collect, value: true }\n";

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

        DataStream<Map<String, Object>> out = in.process(new SpelProcessFunction("test", config));

        out.sinkTo(new PrintSink<>("printSink-> "));

        env.execute();
    }


}
