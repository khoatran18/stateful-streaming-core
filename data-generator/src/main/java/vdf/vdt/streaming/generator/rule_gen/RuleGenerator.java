package vdf.vdt.streaming.generator.rule_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.model.FieldDefinition;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

// Generates structured rule definitions (AST condition trees) for the 30-field dual CDP schema
// (Schema A — transaction events, Schema B — system access logs).
//
// Each rule targets one source (A or B), chosen randomly. All field references within a rule
// use the fully-qualified dot-path format: {source}.v2.{fieldName}  (flat fields)
//                                          {source}.v2.{nestedGroup}.{fieldName}  (nested fields)
//
// Schema A field paths:
//   A.v2.customer_segment    (static categorical — flat)
//   A.v2.age                 (static numeric — flat)
//   A.v2.transaction_type    (dynamic categorical — flat)
//   A.v2.daily_spend_total_vnd  (dynamic numeric — flat)
//   A.v2.debt.transfer_amount_today_vnd  (dynamic numeric — nested in "debt")
//
// Schema B field paths:
//   B.v2.home_province       (static categorical — flat)
//   B.v2.session_duration_seconds  (dynamic numeric — flat)
//   B.v2.risk_signals.fraud_probability_score  (dynamic numeric — nested in "risk_signals")
//
// Static fields allow full operator set (==, !=, <=, >=, <, >).
// Dynamic numeric fields allow inequalities only (<=, >=, <, >).
// Dynamic categorical fields are only emitted inside AND pairs (never standalone).
//
// Window threshold formula:
//   expectedEventsPerId = reqPerSecond * windowSeconds / idRange
//   count threshold     = expectedEventsPerId * U[0.2, 3.0]
//   sum threshold       = expectedEventsPerId * meanValue * U[0.2, 3.0]
//   min/max/avg         = random value within [field.minValue, field.maxValue]
//
// Window expression format (structured object):
//   { field, agg, window: { type, time }, op, threshold }
//   window.time is one of: "2m", "5m", "10m", "15m", "20m", "25m", "30m"
//
// Linear combination (type 3) uses dynamic numeric fields only and may be raw or windowed (50/50).
public class RuleGenerator {

    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx");

    // Window sizes in minutes (2m–30m).
    private static final int[] WINDOW_MINUTES = {2, 5, 10, 15, 20, 25, 30};
    private final String[] windowAggs  = {"sum", "count", "avg", "max", "min"};
    // tumbling = fixed non-overlapping intervals, sliding = overlapping (step < window size)
    private final String[] windowTypes = {"tumbling", "sliding"};
    private final Random random = new Random();

    private final int reqPerSecond;
    private final int idRange;

    public RuleGenerator(int idRange, int reqPerSecond) {
        this.idRange      = Math.max(1, idRange);
        this.reqPerSecond = reqPerSecond;
    }

    // Generates totalRules rules and writes them as a JSON array to filePath.
    // maxUserId - upper bound for random user_id in each rule's metadata (drawn from "user_001" to "user_<maxUserId>").
    // maxTreeDepth - maximum depth for the condition_tree AST (tree depth is randomized between 1 and maxTreeDepth per rule).
    // Each rule has a trigger_criteria source, while condition_tree nodes can independently target source A or B.
    public void generateRulesToFile(int totalRules, String filePath, int maxUserId, int maxTreeDepth) throws IOException {
        List<Map<String, Object>> allRules = new ArrayList<>(totalRules);
        int validMaxDepth = Math.max(1, maxTreeDepth);

        for (int i = 0; i < totalRules; i++) {
            String triggerSrc = random.nextBoolean() ? "A" : "B";
            int targetDepth = 1 + random.nextInt(validMaxDepth);
            Map<String, Object> rule = new LinkedHashMap<>();
            rule.put("rule_id",             "rule_" + triggerSrc + "_" + i);
            rule.put("schema_fields_count", Constants.SCHEMA_A_TOTAL_FIELDS);
            rule.put("metadata",            buildRuleMetadata(maxUserId));
            rule.put("trigger_criteria",     buildTriggerCriteria(triggerSrc));
            rule.put("condition_tree",      generateNode(1, targetDepth));
            allRules.add(rule);
        }

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(filePath), allRules);
        System.out.println("Successfully generated " + totalRules + " rules into: " + filePath);
    }

    public void generateRulesToFile(int totalRules, String filePath, int maxUserId) throws IOException {
        generateRulesToFile(totalRules, filePath, maxUserId, 2);
    }

    // Builds top-level pre-filter trigger criteria (source, version, list of ANDed condition filters).
    // Field names omit the source.version prefix since source and version are explicit fields.
    private Map<String, Object> buildTriggerCriteria(String src) {
        Map<String, Object> trigger = new LinkedHashMap<>();
        trigger.put("source",  src);
        trigger.put("version", "v2");

        List<FieldDefinition> candidatePool = new ArrayList<>();
        candidatePool.addAll(staticCatPool(src));
        candidatePool.addAll(dynCatPool(src));
        candidatePool.addAll(staticNumPool(src));
        candidatePool.addAll(staticBoolPool(src));
        candidatePool.addAll(dynBoolPool(src));
        candidatePool.addAll(dynNumPool(src));

        Collections.shuffle(candidatePool, random);
        int conditionCount = 1 + random.nextInt(3); // 1, 2, or 3 conditions

        List<Map<String, Object>> conditions = new ArrayList<>();
        for (int i = 0; i < Math.min(conditionCount, candidatePool.size()); i++) {
            FieldDefinition fd = candidatePool.get(i);
            Map<String, Object> cond = new LinkedHashMap<>();
            cond.put("field", localFieldPath(src, fd));

            if ("ENUM".equals(fd.getConstraintKind())) {
                String[] ops = {"==", "!=", "IN"};
                String op = ops[random.nextInt(ops.length)];
                cond.put("op", op);
                if ("IN".equals(op)) {
                    List<String> allVals = fd.getEnumValues();
                    int count = Math.min(allVals.size(), 2 + random.nextInt(3));
                    List<String> shuffled = new ArrayList<>(allVals);
                    Collections.shuffle(shuffled, random);
                    cond.put("value", shuffled.subList(0, count));
                } else {
                    cond.put("value", fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size())));
                }
            } else if ("BOOLEAN".equals(fd.getType())) {
                String op = random.nextBoolean() ? "==" : "!=";
                cond.put("op", op);
                cond.put("value", random.nextBoolean());
            } else if ("static_categorical".equals(fd.getCategory()) || "static_numeric".equals(fd.getCategory())) {
                String[] ops = {"==", "!=", "<=", ">=", "<", ">", "IN"};
                String op = ops[random.nextInt(ops.length)];
                cond.put("op", op);
                if ("IN".equals(op)) {
                    int count = 2 + random.nextInt(3);
                    List<Object> vals = new ArrayList<>();
                    for (int k = 0; k < count; k++) {
                        vals.add(formatValue(fd, randomInRange(fd)));
                    }
                    cond.put("value", vals);
                } else {
                    cond.put("value", formatValue(fd, randomInRange(fd)));
                }
            } else { // dynamic numeric
                String[] ops = {"<=", ">=", "<", ">"};
                String op = ops[random.nextInt(ops.length)];
                cond.put("op", op);
                cond.put("value", formatValue(fd, randomInRange(fd)));
            }

            conditions.add(cond);
        }

        trigger.put("conditions", conditions);
        return trigger;
    }

    private Object formatValue(FieldDefinition fd, double value) {
        if ("INT".equals(fd.getType())) {
            return (int) Math.round(value);
        } else if ("FLOAT".equals(fd.getType())) {
            return Double.parseDouble(String.format(Locale.US, "%.2f", value));
        }
        return String.format(Locale.US, "%.2f", value);
    }

    // Builds the metadata block for one rule.
    // event_time - ISO-8601 current time; user_id - random "user_001".."user_<maxUserId>".
    private Map<String, Object> buildRuleMetadata(int maxUserId) {
        int userId = 1 + random.nextInt(Math.max(1, maxUserId));
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("event_time", OffsetDateTime.now().format(TIMESTAMP_FMT));
        meta.put("user_id",    String.format("user_%03d", userId));
        return meta;
    }

    // Recursively builds one AST node. Depth is randomised between 1 and 2 total levels.
    // Each node independently picks a target source ("A" or "B") for its expressions.
    private Map<String, Object> generateNode(int currentDepth, int maxDepth) {
        String nodeSrc = random.nextBoolean() ? "A" : "B";
        if (currentDepth >= maxDepth || random.nextBoolean()) {
            // ~20% of leaf positions: dynCat paired with window agg in an AND node.
            if (random.nextInt(5) == 0) {
                return buildDynCatPairNode(nodeSrc);
            }
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("type",       "CONDITION");
            node.put("expression", generateExpression(nodeSrc));
            return node;
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("type", random.nextBoolean() ? "AND" : "OR");
        node.put("children", List.of(
                generateNode(currentDepth + 1, maxDepth),
                generateNode(currentDepth + 1, maxDepth)));
        return node;
    }

    // Picks one of five expression types at random.
    // Returns String for categorical / numeric / boolean / raw linear; Map for window agg / windowed linear.
    private Object generateExpression(String src) {
        return switch (random.nextInt(5)) {
            case 0  -> buildCategoricalExpr(src);
            case 1  -> buildRawNumericExpr(src);
            case 2  -> buildWindowAggExprMap(src);
            case 3  -> buildBooleanExpr(src);
            default -> buildLinearCombinationExpr(src);
        };
    }

    // ── Path helpers ──────────────────────────────────────────────────────────

    // Unified field path helper: returns {src}.v2.{name} for root fields,
    // or {src}.v2.{group}.{name} for fields belonging to a nested group.
    private String fieldPath(String src, FieldDefinition fd) {
        Set<String> nestedNames = "A".equals(src)
                ? Constants.SCHEMA_A_NESTED_DYNAMIC_FIELD_NAMES
                : Constants.SCHEMA_B_NESTED_DYNAMIC_FIELD_NAMES;
        if (nestedNames.contains(fd.getName())) {
            String group = "A".equals(src)
                    ? Constants.SCHEMA_A_NESTED_DYNAMIC_GROUP
                    : Constants.SCHEMA_B_NESTED_DYNAMIC_GROUP;
            return src + ".v2." + group + "." + fd.getName();
        }
        return src + ".v2." + fd.getName();
    }

    // Local field path helper for trigger_criteria (omitting {src}.v2. prefix):
    // returns {name} for root fields, or {group}.{name} for fields in a nested group.
    private String localFieldPath(String src, FieldDefinition fd) {
        Set<String> nestedNames = "A".equals(src)
                ? Constants.SCHEMA_A_NESTED_DYNAMIC_FIELD_NAMES
                : Constants.SCHEMA_B_NESTED_DYNAMIC_FIELD_NAMES;
        if (nestedNames.contains(fd.getName())) {
            String group = "A".equals(src)
                    ? Constants.SCHEMA_A_NESTED_DYNAMIC_GROUP
                    : Constants.SCHEMA_B_NESTED_DYNAMIC_GROUP;
            return group + "." + fd.getName();
        }
        return fd.getName();
    }

    // ── Field pool helpers ────────────────────────────────────────────────────

    private List<FieldDefinition> staticCatPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_STATIC_CATEGORICAL_FIELDS
                               : Constants.SCHEMA_B_STATIC_CATEGORICAL_FIELDS;
    }

    private List<FieldDefinition> dynCatPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_DYNAMIC_CATEGORICAL_FIELDS
                               : Constants.SCHEMA_B_DYNAMIC_CATEGORICAL_FIELDS;
    }

    private List<FieldDefinition> staticNumPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_STATIC_NUMERIC_FIELDS
                               : Constants.SCHEMA_B_STATIC_NUMERIC_FIELDS;
    }

    private List<FieldDefinition> dynNumPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_DYNAMIC_NUMERIC_FIELDS
                               : Constants.SCHEMA_B_DYNAMIC_NUMERIC_FIELDS;
    }

    private List<FieldDefinition> staticBoolPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_STATIC_BOOLEAN_FIELDS
                               : Constants.SCHEMA_B_STATIC_BOOLEAN_FIELDS;
    }

    private List<FieldDefinition> dynBoolPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_DYNAMIC_BOOLEAN_FIELDS
                               : Constants.SCHEMA_B_DYNAMIC_BOOLEAN_FIELDS;
    }

    // ── Expression builders ───────────────────────────────────────────────────

    // Type 3: boolean expression using static or dynamic boolean fields.
    // Examples:
    //   A.v2.is_vip_member == true
    //   B.v2.risk_signals.is_suspicious_ip == true
    private String buildBooleanExpr(String src) {
        boolean useStatic = random.nextBoolean();
        List<FieldDefinition> pool = useStatic ? staticBoolPool(src) : dynBoolPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));

        String op   = random.nextBoolean() ? "==" : "!=";
        boolean val = random.nextBoolean();

        return fieldPath(src, fd) + " " + op + " " + val;
    }

    // Type 0: static categorical expression.
    // Uses static categorical fields for the chosen source (all are flat at event root).
    // Operators: ==, !=, or IN. Value picked from the field's actual enum_values.
    // Examples:
    //   A.v2.loyalty_tier == 'GOLD'
    //   A.v2.customer_segment IN ['PREMIUM', 'VIP']
    //   B.v2.home_province != 'HANOI'
    private String buildCategoricalExpr(String src) {
        List<FieldDefinition> pool = staticCatPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String[] ops = {"==", "!=", "IN"};
        String op = ops[random.nextInt(ops.length)];

        if ("IN".equals(op)) {
            List<String> allVals = fd.getEnumValues();
            int count = Math.min(allVals.size(), 2 + random.nextInt(3));
            List<String> shuffled = new ArrayList<>(allVals);
            Collections.shuffle(shuffled, random);
            String inListStr = shuffled.subList(0, count).stream()
                    .map(v -> "'" + v + "'")
                    .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
            return fieldPath(src, fd) + " IN " + inListStr;
        }

        String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
        return fieldPath(src, fd) + " " + op + " '" + val + "'";
    }

    // Type 1: raw numeric expression using static or dynamic numeric fields.
    // Static fields allow full operator set (==, !=, <=, >=, <, >, IN).
    // Dynamic fields allow inequalities only (<=, >=, <, >).
    // Examples:
    //   A.v2.age >= 35                                 (static, flat)
    //   A.v2.age IN [25, 30, 35]                       (static, flat)
    //   A.v2.daily_spend_total_vnd > 50000000.00       (dynamic, flat)
    //   B.v2.risk_signals.fraud_probability_score >= 75.00  (dynamic, nested)
    private String buildRawNumericExpr(String src) {
        boolean useStatic = random.nextBoolean();
        FieldDefinition fd;
        String fieldPathStr;
        String[] ops;

        if (useStatic) {
            List<FieldDefinition> pool = staticNumPool(src);
            fd           = pool.get(random.nextInt(pool.size()));
            fieldPathStr = fieldPath(src, fd);        // static numeric fields are flat
            ops          = new String[]{"==", "!=", "<=", ">=", "<", ">", "IN"};
        } else {
            List<FieldDefinition> pool = dynNumPool(src);
            fd           = pool.get(random.nextInt(pool.size()));
            fieldPathStr = fieldPath(src, fd);        // flat or nested depending on field
            ops          = new String[]{"<=", ">=", "<", ">"};
        }

        String op = ops[random.nextInt(ops.length)];
        if ("IN".equals(op)) {
            int count = 2 + random.nextInt(3);
            List<String> valStrs = new ArrayList<>();
            for (int k = 0; k < count; k++) {
                valStrs.add(formatThreshold(fd, randomInRange(fd)));
            }
            return fieldPathStr + " IN [" + String.join(", ", valStrs) + "]";
        }

        double threshold = randomInRange(fd);
        return fieldPathStr + " " + op + " " + formatThreshold(fd, threshold);
    }

    // Type 2: window aggregation expression using dynamic numeric fields.
    // field in the output map is the full dot-path (flat or nested) of the aggregated field.
    // Threshold is derived from expected event count in the window per customer ID.
    // Example output:
    //   { field: "A.v2.daily_spend_total_vnd", agg: "sum",
    //     window: { type: "tumbling", time: "10m" }, op: ">=", threshold: 50000000.00 }
    //   { field: "B.v2.risk_signals.fraud_probability_score", agg: "avg",
    //     window: { type: "sliding", time: "5m" }, op: ">=", threshold: 75.00 }
    private Map<String, Object> buildWindowAggExprMap(String src) {
        List<FieldDefinition> pool = dynNumPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));

        String agg     = windowAggs[random.nextInt(windowAggs.length)];
        String winType = windowTypes[random.nextInt(windowTypes.length)];
        String[] ops   = {"<=", ">=", "<", ">"};
        String op      = ops[random.nextInt(ops.length)];

        int    windowMin   = WINDOW_MINUTES[random.nextInt(WINDOW_MINUTES.length)];
        String windowLabel = windowMin + "m";

        double windowSeconds       = windowMin * 60.0;
        double expectedEventsPerId = Math.max(0.1, (double) reqPerSecond * windowSeconds / idRange);
        double meanValue           = (fd.getMinValue() + fd.getMaxValue()) / 2.0;

        double threshold = switch (agg) {
            case "count" -> expectedEventsPerId * (0.2 + random.nextDouble() * 2.8);
            case "sum"   -> expectedEventsPerId * meanValue * (0.2 + random.nextDouble() * 2.8);
            default      -> randomInRange(fd); // min, max, avg -> value within field range
        };

        Map<String, Object> window = new LinkedHashMap<>();
        window.put("type", winType);
        window.put("time", windowLabel);

        Map<String, Object> expr = new LinkedHashMap<>();
        expr.put("field",     fieldPath(src, fd));    // flat or nested path
        expr.put("agg",       agg);
        expr.put("window",    window);
        expr.put("op",        op);
        expr.put("threshold", Double.parseDouble(String.format(Locale.US, "%.2f", threshold)));
        return expr;
    }

    // Builds an AND node pairing a dynamic categorical filter with a window aggregation.
    // Dynamic categorical fields are flat or nested depending on field.
    // Operators for dynCat: ==, !=, or IN.
    // Example dynCat expression:
    //   A.v2.transaction_type == 'TRANSFER'
    //   A.v2.debt.loan_repayment_status IN ['OVERDUE_1_30', 'OVERDUE_31_90']
    private Map<String, Object> buildDynCatPairNode(String src) {
        List<FieldDefinition> pool = dynCatPool(src);
        FieldDefinition fd  = pool.get(random.nextInt(pool.size()));
        String[] ops = {"==", "!=", "IN"};
        String op = ops[random.nextInt(ops.length)];
        String dynCatExpr;

        if ("IN".equals(op)) {
            List<String> allVals = fd.getEnumValues();
            int count = Math.min(allVals.size(), 2 + random.nextInt(3));
            List<String> shuffled = new ArrayList<>(allVals);
            Collections.shuffle(shuffled, random);
            String inListStr = shuffled.subList(0, count).stream()
                    .map(v -> "'" + v + "'")
                    .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
            dynCatExpr = fieldPath(src, fd) + " IN " + inListStr;
        } else {
            String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
            dynCatExpr = fieldPath(src, fd) + " " + op + " '" + val + "'";
        }

        Map<String, Object> dynCatNode = new LinkedHashMap<>();
        dynCatNode.put("type",       "CONDITION");
        dynCatNode.put("expression", dynCatExpr);

        // Window aggregation CONDITION child — the meaningful computation over filtered events.
        Map<String, Object> windowNode = new LinkedHashMap<>();
        windowNode.put("type",       "CONDITION");
        windowNode.put("expression", buildWindowAggExprMap(src));

        Map<String, Object> andNode = new LinkedHashMap<>();
        andNode.put("type",     "AND");
        andNode.put("children", List.of(dynCatNode, windowNode));
        return andNode;
    }

    // Type 4: linear combination of two DYNAMIC numeric fields from the same source sharing the SAME range.
    // Random weights w1 and w2 such that w1 + w2 = 1.0 (e.g. 0.65 and 0.35).
    // Example formula:
    //   (A.v2.daily_spend_total_vnd * 0.65 + A.v2.debt.transfer_amount_today_vnd * 0.35) >= 50000000.00
    private Object buildLinearCombinationExpr(String src) {
        List<FieldDefinition> pool = dynNumPool(src);
        List<FieldDefinition> shuffledPool = new ArrayList<>(pool);
        Collections.shuffle(shuffledPool, random);

        FieldDefinition fd1 = shuffledPool.get(0);
        FieldDefinition fd2 = shuffledPool.get(0);

        for (FieldDefinition candidate : shuffledPool) {
            List<FieldDefinition> matches = pool.stream()
                    .filter(fd -> fd.getMinValue().equals(candidate.getMinValue())
                               && fd.getMaxValue().equals(candidate.getMaxValue()))
                    .toList();
            if (matches.size() >= 2) {
                fd1 = candidate;
                List<FieldDefinition> otherMatches = matches.stream()
                        .filter(fd -> !fd.getName().equals(candidate.getName()))
                        .toList();
                fd2 = otherMatches.get(random.nextInt(otherMatches.size()));
                break;
            }
        }

        // Random weights w1 and w2 between 0.05 and 0.95, w1 + w2 = 1.0
        double w1 = (5 + random.nextInt(91)) / 100.0;
        double w2 = Math.round((1.0 - w1) * 100.0) / 100.0;

        String fieldFormula = String.format(Locale.US, "(%s * %.2f + %s * %.2f)",
                fieldPath(src, fd1), w1, fieldPath(src, fd2), w2);

        String[] ops = {"<=", ">=", "<", ">"};
        String op = ops[random.nextInt(ops.length)];

        // Windowed variant: wrap the formula as the field in a window agg object.
        if (random.nextBoolean()) {
            String agg     = windowAggs[random.nextInt(windowAggs.length)];
            String winType = windowTypes[random.nextInt(windowTypes.length)];
            int    windowMin   = WINDOW_MINUTES[random.nextInt(WINDOW_MINUTES.length)];

            double windowSeconds       = windowMin * 60.0;
            double expectedEventsPerId = Math.max(0.1, (double) reqPerSecond * windowSeconds / idRange);
            double meanValue           = (fd1.getMinValue() + fd1.getMaxValue()) / 2.0;

            double threshold = switch (agg) {
                case "count" -> expectedEventsPerId * (0.2 + random.nextDouble() * 2.8);
                case "sum"   -> expectedEventsPerId * meanValue * (0.2 + random.nextDouble() * 2.8);
                default      -> randomInRange(fd1);
            };

            Map<String, Object> window = new LinkedHashMap<>();
            window.put("type", winType);
            window.put("time", windowMin + "m");

            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field",     fieldFormula);
            expr.put("agg",       agg);
            expr.put("window",    window);
            expr.put("op",        op);
            expr.put("threshold", Double.parseDouble(String.format(Locale.US, "%.2f", threshold)));
            return expr;
        }

        // Raw variant: plain string expression.
        double threshold = randomInRange(fd1);
        return fieldFormula + " " + op + " " + formatThreshold(fd1, threshold);
    }

    private double randomInRange(FieldDefinition fd) {
        return fd.getMinValue() + (fd.getMaxValue() - fd.getMinValue()) * random.nextDouble();
    }

    // INT fields are formatted without decimals for readability.
    private String formatThreshold(FieldDefinition fd, double value) {
        if ("INT".equals(fd.getType())) {
            return String.format(Locale.US, "%.0f", value);
        }
        return String.format(Locale.US, "%.2f", value);
    }
}