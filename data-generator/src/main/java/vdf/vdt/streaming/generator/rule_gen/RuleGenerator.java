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
    // Each rule randomly targets source A or B. All field references use dot-path notation.
    public void generateRulesToFile(int totalRules, String filePath, int maxUserId) throws IOException {
        List<Map<String, Object>> allRules = new ArrayList<>(totalRules);

        for (int i = 0; i < totalRules; i++) {
            String src = random.nextBoolean() ? "A" : "B";
            Map<String, Object> rule = new LinkedHashMap<>();
            rule.put("rule_id",             "rule_" + src + "_" + i);
            rule.put("schema_fields_count", Constants.SCHEMA_A_TOTAL_FIELDS);
            rule.put("metadata",            buildRuleMetadata(maxUserId));
            rule.put("condition_tree",      generateNode(1, random.nextInt(4) + 2, src));
            allRules.add(rule);
        }

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(filePath), allRules);
        System.out.println("Successfully generated " + totalRules + " rules into: " + filePath);
    }

    // Builds the metadata block for one rule.
    // timestamp - ISO-8601 current time; user_id - random "user_001".."user_<maxUserId>".
    private Map<String, Object> buildRuleMetadata(int maxUserId) {
        int userId = 1 + random.nextInt(Math.max(1, maxUserId));
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("timestamp", OffsetDateTime.now().format(TIMESTAMP_FMT));
        meta.put("user_id",   String.format("user_%03d", userId));
        return meta;
    }

    // Recursively builds one AST node. Depth is randomised between 2 and 5 total levels.
    // At leaf positions (~20% chance) a dynamic-categorical+window AND pair is injected instead
    // of a simple CONDITION, ensuring dynCat expressions are never standalone.
    // src - the source schema ("A" or "B") that all field references in this subtree must target.
    private Map<String, Object> generateNode(int currentDepth, int maxDepth, String src) {
        if (currentDepth >= maxDepth || random.nextBoolean()) {
            // ~20% of leaf positions: dynCat paired with window agg in an AND node.
            if (random.nextInt(5) == 0) {
                return buildDynCatPairNode(src);
            }
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("type",       "CONDITION");
            node.put("expression", generateExpression(src));
            return node;
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("type", random.nextBoolean() ? "AND" : "OR");
        node.put("children", List.of(
                generateNode(currentDepth + 1, maxDepth, src),
                generateNode(currentDepth + 1, maxDepth, src)));
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
    // Operators: == or !=. Value picked from the field's actual enum_values.
    // Dynamic categorical fields are excluded — see buildDynCatPairNode.
    // Examples:
    //   A.v2.loyalty_tier == 'GOLD'
    //   B.v2.home_province != 'HANOI'
    private String buildCategoricalExpr(String src) {
        List<FieldDefinition> pool = staticCatPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String op  = random.nextBoolean() ? "==" : "!=";
        String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
        return fieldPath(src, fd) + " " + op + " '" + val + "'";
    }

    // Type 1: raw numeric expression using static or dynamic numeric fields.
    // Static fields allow full operator set (==, !=, <=, >=, <, >).
    // Dynamic fields allow inequalities only (<=, >=, <, >).
    // Threshold is a random value within [field.minValue, field.maxValue].
    // Examples:
    //   A.v2.age >= 35                                 (static, flat)
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
            ops          = new String[]{"==", "!=", "<=", ">=", "<", ">"};
        } else {
            List<FieldDefinition> pool = dynNumPool(src);
            fd           = pool.get(random.nextInt(pool.size()));
            fieldPathStr = fieldPath(src, fd);        // flat or nested depending on field
            ops          = new String[]{"<=", ">=", "<", ">"};
        }

        String op        = ops[random.nextInt(ops.length)];
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
    // Operators for dynCat: == or != only (categorical matching, no ordering).
    // Example dynCat expression:
    //   A.v2.transaction_type == 'TRANSFER'
    //   A.v2.debt.loan_repayment_status == 'OVERDUE_31_90'
    private Map<String, Object> buildDynCatPairNode(String src) {
        List<FieldDefinition> pool = dynCatPool(src);
        FieldDefinition fd  = pool.get(random.nextInt(pool.size()));
        String op  = random.nextBoolean() ? "==" : "!=";
        String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
        String dynCatExpr = fieldPath(src, fd) + " " + op + " '" + val + "'";

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

    // Type 3: linear combination of two DYNAMIC numeric fields from the same source.
    // field formula uses fieldPath — either flat or nested depending on the field.
    //
    // Raw example:
    //   (A.v2.daily_spend_total_vnd * 0.7 + A.v2.debt.transfer_amount_today_vnd * 0.3) >= 50000000.00
    // Windowed example:
    //   { field: "(A.v2.daily_spend_total_vnd * 0.7 + A.v2.debt.transfer_amount_today_vnd * 0.3)",
    //     agg: "sum", window: { type: "tumbling", time: "10m" }, op: ">=", threshold: ... }
    private Object buildLinearCombinationExpr(String src) {
        List<FieldDefinition> pool = dynNumPool(src);
        FieldDefinition fd1 = pool.get(random.nextInt(pool.size()));
        FieldDefinition fd2 = pool.get(random.nextInt(pool.size()));

        String[] ops = {"<=", ">=", "<", ">"};
        String op = ops[random.nextInt(ops.length)];

        // Use fieldPath so nested fields get the correct nested path.
        String fieldFormula = "(" + fieldPath(src, fd1) + " * 0.7 + " + fieldPath(src, fd2) + " * 0.3)";

        // Windowed variant: wrap the formula as the field in a window agg object.
        if (random.nextBoolean()) {
            String agg     = windowAggs[random.nextInt(windowAggs.length)];
            String winType = windowTypes[random.nextInt(windowTypes.length)];
            int    windowMin   = WINDOW_MINUTES[random.nextInt(WINDOW_MINUTES.length)];

            double windowSeconds       = windowMin * 60.0;
            double expectedEventsPerId = Math.max(0.1, (double) reqPerSecond * windowSeconds / idRange);
            double meanValue           = (fd1.getMinValue() + fd1.getMaxValue()) / 2.0
                                       + (fd2.getMinValue() + fd2.getMaxValue()) / 2.0;

            double threshold = switch (agg) {
                case "count" -> expectedEventsPerId * (0.2 + random.nextDouble() * 2.8);
                case "sum"   -> expectedEventsPerId * meanValue * (0.2 + random.nextDouble() * 2.8);
                default      -> {
                    double lo = Math.min(fd1.getMinValue(), fd2.getMinValue());
                    double hi = Math.max(fd1.getMaxValue(), fd2.getMaxValue());
                    yield lo + (hi - lo) * random.nextDouble();
                }
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
        double maxOfTwo = Math.max(fd1.getMaxValue(), fd2.getMaxValue());
        double minOfTwo = Math.min(fd1.getMinValue(), fd2.getMinValue());
        double threshold = minOfTwo + (maxOfTwo - minOfTwo) * random.nextDouble();
        return fieldFormula + " " + op + " " + String.format(Locale.US, "%.2f", threshold);
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