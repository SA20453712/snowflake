package com.apple.snowflakemigration.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

@Component
public class JSONUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String[] parseJsonColumns(JsonNode jsonNode) throws IOException {
        Iterator<String> fieldNames = jsonNode.fieldNames();
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(fieldNames, Spliterator.ORDERED), false)
                .map(field -> field.replace("~", ""))
                .toArray(String[]::new);
    }

    public static String[] parseJsonValues(JsonNode jsonNode)  {
        List<String> resultList = new ArrayList<>();

        // Iterate through all fields in the JSON node
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode node = field.getValue();

            if (node.isArray()) {
                // If the field is an array, convert it to a comma-separated string
                StringBuilder arrayAsString = new StringBuilder();
                for (JsonNode arrayElement : node) {
                    if (arrayAsString.length() > 0) {
                        arrayAsString.append("| ");
                    }
                    arrayAsString.append(arrayElement.asText());
                }
                resultList.add(arrayAsString.toString());
            } else {
                // If the field is not an array, just add its string value
                resultList.add(node.asText());
            }

        }
        return resultList.toArray(new String[0]);
    }
}
