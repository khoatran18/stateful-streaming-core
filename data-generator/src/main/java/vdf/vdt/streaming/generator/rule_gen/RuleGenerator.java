package vdf.vdt.streaming.generator.rule_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.vdf.streaming.generator.common.Constants;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class RuleGenerator {
    private final List<Integer> schemaSizes = List.of(20, 50, 100, 150, 200);
    private final String[] timeWindows = {"5s", "10s", "30s", "1m", "5m", "10m", "1h"};
    private final String[] windowAggs = {"sum", "count", "avg", "max", "min"};
    private final Random random = new Random();

    public void generateRulesToFile(int totalRules, String filePath) throws IOException {
        List<Map<String, Object>> allRules = new ArrayList<>();
        int rulesPerSchema = totalRules / 5;

        for (int size : schemaSizes) {
            for (int i = 0; i < rulesPerSchema; i++) {
                Map<String, Object> rule = new LinkedHashMap<>();
                rule.put("rule_id", "rule_" + size + "_" + i);
                rule.put("schema_fields_count", size);

                // Độ cao cây điều kiện random từ 2 đến 5
                int maxDepth = random.nextInt(4) + 2;
                rule.put("condition_tree", generateNode(size, 1, maxDepth));

                allRules.add(rule);
            }
        }

        // Ghi ra file JSON tại địa chỉ được chỉ định
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(filePath), allRules);
        System.out.println("Successfully generated " + totalRules + " rules into: " + filePath);
    }

    private Map<String, Object> generateNode(int schemaSize, int currentDepth, int maxDepth) {
        Map<String, Object> node = new LinkedHashMap<>();

        if (currentDepth >= maxDepth || random.nextBoolean()) {
            // Leaf node: Biểu thức đơn
            node.put("type", "CONDITION");
            node.put("expression", generateExpression(schemaSize));
        } else {
            // Internal node: AND / OR
            node.put("type", random.nextBoolean() ? "AND" : "OR");
            List<Map<String, Object>> children = new ArrayList<>();
            children.add(generateNode(schemaSize, currentDepth + 1, maxDepth));
            children.add(generateNode(schemaSize, currentDepth + 1, maxDepth));
            node.put("children", children);
        }
        return node;
    }

    private String generateExpression(int schemaSize) {
        List<String> numCols = Constants.NUMERIC_COLUMNS.get(schemaSize);
        List<String> catCols = Constants.CATEGORICAL_COLUMNS.get(schemaSize);

        int exprType = random.nextInt(4); // Phân bổ đều các loại biểu thức

        if (exprType == 0 && !catCols.isEmpty()) {
            // Biểu thức định danh: chỉ có == hoặc !=
            String col = catCols.get(random.nextInt(catCols.size()));
            String op = random.nextBoolean() ? "==" : "!=";
            return col + " " + op + " 'val_" + random.nextInt(5) + "'";
        }
        else if (exprType == 1 && !numCols.isEmpty()) {
            // Biểu thức số dạng gốc: có ==, !=, <=, >=, <, >
            String col = numCols.get(random.nextInt(numCols.size()));
            String[] ops = {"==", "!=", "<=", ">=", "<", ">"};
            String op = ops[random.nextInt(ops.length)];
            return col + " " + op + " " + random.nextInt(100);
        }
        else if (exprType == 2 && !numCols.isEmpty()) {
            // 5 dạng cửa sổ (sum/count/avg/max/min) kết hợp thời gian (s, m, h)
            String col = numCols.get(random.nextInt(numCols.size()));
            String agg = windowAggs[random.nextInt(windowAggs.length)];
            String time = timeWindows[random.nextInt(timeWindows.length)];
            String fieldExpr = col + "_" + agg + "_" + time;

            // Các biểu thức số cửa sổ chỉ có <= >= < >
            String[] ops = {"<=", ">=", "<", ">"};
            String op = ops[random.nextInt(ops.length)];
            return fieldExpr + " " + op + " " + (random.nextInt(1000) + 10);
        }
        else {
            // Biểu thức tuyến tính kết hợp các cột số
            if (numCols.size() < 2) return "s1_num_age > 18";
            String c1 = numCols.get(random.nextInt(numCols.size()));
            String c2 = numCols.get(random.nextInt(numCols.size()));
            String[] ops = {"<=", ">=", "<", ">"};
            String op = ops[random.nextInt(ops.length)];
            return "(" + c1 + " * 0.7 + " + c2 + " * 0.3) " + op + " " + random.nextInt(500);
        }
    }
}