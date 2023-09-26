package net.hoteljuliet.flinkspel;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void testMermaid() throws JsonProcessingException {

        String plan = "{\n" +
                "  \"nodes\" : [ {\n" +
                "    \"id\" : 1,\n" +
                "    \"type\" : \"Source: source\",\n" +
                "    \"pact\" : \"Data Source\",\n" +
                "    \"contents\" : \"Source: source\",\n" +
                "    \"parallelism\" : 1\n" +
                "  }, {\n" +
                "    \"id\" : 2,\n" +
                "    \"type\" : \"spelProcess\",\n" +
                "    \"pact\" : \"Operator\",\n" +
                "    \"contents\" : \"spelProcess\",\n" +
                "    \"parallelism\" : 8,\n" +
                "    \"predecessors\" : [ {\n" +
                "      \"id\" : 1,\n" +
                "      \"ship_strategy\" : \"REBALANCE\",\n" +
                "      \"side\" : \"second\"\n" +
                "    } ]\n" +
                "  }, {\n" +
                "    \"id\" : 4,\n" +
                "    \"type\" : \"print: Writer\",\n" +
                "    \"pact\" : \"Operator\",\n" +
                "    \"contents\" : \"print: Writer\",\n" +
                "    \"parallelism\" : 8,\n" +
                "    \"predecessors\" : [ {\n" +
                "      \"id\" : 2,\n" +
                "      \"ship_strategy\" : \"FORWARD\",\n" +
                "      \"side\" : \"second\"\n" +
                "    } ]\n" +
                "  } ]\n" +
                "}";

        String mermaid = Utils.planToMermaid(plan);
        System.out.println(mermaid);
    }
}
