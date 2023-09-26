package net.hoteljuliet.flinkspel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.hoteljuliet.spel.Context;

import java.util.List;
import java.util.Map;

public class Utils {

    public static String planToMermaid(String plan) throws JsonProcessingException {

        StringBuilder stringBuilder = new StringBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> planMap = objectMapper.readValue(plan, Map.class);
        Context planContext = new Context(planMap);

        List<Map<String, Object>> nodes = planContext.getField("nodes");

        for (Map<String, Object> node : nodes) {
            Context nodeContext = new Context(node);
            Integer id = nodeContext.getField("id");
            String content = nodeContext.getField("contents");
            Integer parallelism = nodeContext.getField("parallelism");
            String description = content.strip().toLowerCase() + "\\n" + "parallelism:" + parallelism;
            if (nodeContext.hasField("predecessors")) {
                stringBuilder.append(id + "[" + description + "]").append("\n");
                List<Map<String, Object>> predecessors = nodeContext.getField("predecessors");

                for (Map<String, Object> predecessor : predecessors) {
                    Context predecessorContext = new Context(predecessor);
                    Integer predecessorId = predecessorContext.getField("id");
                    String shipStrategy = predecessorContext.getField("ship_strategy");
                    stringBuilder.append(predecessorId + " --" + shipStrategy + " --> " + id).append("\n");
                }
            }
            else {
                // this is a source
                stringBuilder.append(id + "((" + description + "))").append("\n");
            }
        }
        return stringBuilder.toString();
    }
}
