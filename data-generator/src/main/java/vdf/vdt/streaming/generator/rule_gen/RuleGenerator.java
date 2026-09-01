package vdf.vdt.streaming.generator.rule_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.model.FieldDefinition;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

// Generates structured rule definitions (AST condition trees) for the 34-field dual CDP schema
// (Schema A — transaction events, Schema B — system access logs).
//
// Each rule has trigger_criteria as an array of 1-2 source-version entries (multi-source support).
// Both sources share the same schema_version read from application.properties.
// All field references within condition_tree use the fully-qualified dot-path format:
//   {source}.{version}.{fieldName}          (flat fields)
//   {source}.{version}.{nestedGroup}.{field} (nested fields)
// filter fields inside window use LOCAL paths (no source.version prefix).
//
// trigger_criteria.conditions is a 2D list (list-of-lists):
//   Outer list: OR semantics — event passes if it satisfies ALL conditions in ANY inner list.
//   Inner list: AND semantics — all conditions in the inner list must hold simultaneously.
//   Example: [[cond1, cond2], [cond3]] means (cond1 AND cond2) OR cond3.
//
// filter inside window expressions is a list of conditions (AND semantics):
//   All conditions in the list must hold for a historical event to be included in the aggregation.
//   Example: [{field: "txn_type", op: "==", value: "TRANSFER"}, {field: "channel", op: "!=", value: "ATM"}]
//
// CONDITION nodes in condition_tree carry an "is_window" boolean:
//   true  — expression contains a window aggregation (field/expr + agg + window + filter + op + threshold).
//   false — plain scalar comparison (no window involved).
//
// Expression types generated (random pick among 7):
//   0 - Categorical (STRING): ==, !=, IN, NOT IN; rhs: literal or right_field
//   1 - Numeric (INT/LONG/FLOAT/DOUBLE): ==, !=, >, <, >=, <=, BETWEEN, IN, NOT IN;
//       rhs: literal, right_field, or inline (for simple cases)
//   2 - Window aggregation: SUM/AVG/MAX/MIN/COUNT over sliding or tumbling window;
//       mandatory filter list; window uses { type, duration [, slide] } format
//   3 - Boolean: ==, !=; rhs: literal or right_field
//   4 - Linear combination: weighted sum of two dynamic-numeric fields; raw or windowed
//   5 - Timestamp: ==, !=, >, <, >=, <=, BETWEEN; rhs: ISO-8601 literal or right_field
//   6 - Expr-lhs: arithmetic expression as left side; rhs: literal, right_field, or right_expr
//
// Operator coverage per DATA_TYPE.md:
//   INT/LONG  — ==, !=, >, <, >=, <=, BETWEEN, IN, NOT IN, arithmetic (+,-,*,/,%)
//   FLOAT/DOUBLE — ==, !=, >, <, >=, <=, BETWEEN, arithmetic (+,-,*,/)
//   STRING    — ==, !=, IN, NOT IN
//   BOOLEAN   — ==, !=
//   TIMESTAMP — ==, !=, >, <, >=, <=, BETWEEN
//
// Right-hand side per DATA_TYPE.md:
//   IN, NOT IN, BETWEEN — always literal (no right_field inside list ops)
//   others              — literal, right_field, or expr depending on expression builder
//
// Window threshold formula:
//   expectedEventsPerId = reqPerSecond * windowSeconds / idRange
//   count threshold     = expectedEventsPerId * U[0.2, 3.0]
//   sum threshold       = expectedEventsPerId * meanValue * U[0.2, 3.0]
//   min/max/avg         = random value within [field.minValue, field.maxValue]
public class RuleGenerator {

    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");

    // Window duration options in minutes.
    private static final int[] WINDOW_MINUTES = {5, 10, 15, 20, 25, 30, 60};
    // Slide options for sliding windows in minutes (picked as a factor of the chosen duration).
    private static final int[] SLIDE_MINUTES  = {1, 2, 5, 10};

    private final String[] windowAggs  = {"sum", "count", "avg", "max", "min"};
    private final String[] windowTypes = {"tumbling", "sliding"};
    private final Random random = new Random();

    private final int reqPerSecond;
    private final int idRange;
    // Schema version read from application.properties (e.g. "v2").
    // Both source A and source B in trigger_criteria use this same version.
    private final String schemaVersion;

    public RuleGenerator(int idRange, int reqPerSecond, String schemaVersion) {
        this.idRange       = Math.max(1, idRange);
        this.reqPerSecond  = reqPerSecond;
        this.schemaVersion = schemaVersion;
    }

    /** Backwards-compatible constructor that defaults schemaVersion to "v2". */
    public RuleGenerator(int idRange, int reqPerSecond) {
        this(idRange, reqPerSecond, "v2");
    }

    // Generates totalRules rules and writes them as a JSON array to filePath.
    // maxUserId - upper bound for random user_id in each rule's metadata.
    // maxTreeDepth - maximum depth for the condition_tree AST (randomised 1..maxTreeDepth per rule).
    public void generateRulesToFile(int totalRules, String filePath, int maxUserId, int maxTreeDepth) throws IOException {
        List<Map<String, Object>> allRules = new ArrayList<>(totalRules);
        int validMaxDepth = Math.max(1, maxTreeDepth);

        for (int i = 0; i < totalRules; i++) {
            String triggerSrc = random.nextBoolean() ? "A" : "B";
            int targetDepth = 1 + random.nextInt(validMaxDepth);
            Map<String, Object> rule = new LinkedHashMap<>();
            rule.put("rule_id",            "rule_" + triggerSrc + "_" + i);
            rule.put("schema_fields_count", Constants.SCHEMA_A_TOTAL_FIELDS);
            rule.put("metadata",           buildRuleMetadata(maxUserId));
            rule.put("trigger_criteria",   buildTriggerCriteriaList(triggerSrc));
            rule.put("condition_tree",     generateNode(1, targetDepth));
            allRules.add(rule);
        }

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(filePath), allRules);
        System.out.println("Successfully generated " + totalRules + " rules into: " + filePath);
    }

    public void generateRulesToFile(int totalRules, String filePath, int maxUserId) throws IOException {
        generateRulesToFile(totalRules, filePath, maxUserId, 2);
    }

    // ── Trigger criteria ──────────────────────────────────────────────────────

    // Builds trigger_criteria as an array of 1 or 2 source-version objects (50/50 chance).
    // Both entries use the same schemaVersion from application.properties.
    // The second entry (50% chance) uses the complementary source (A↔B).
    private List<Map<String, Object>> buildTriggerCriteriaList(String primarySrc) {
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(buildOneTriggerCriteria(primarySrc, schemaVersion));
        if (random.nextBoolean()) {
            String otherSrc = "A".equals(primarySrc) ? "B" : "A";
            list.add(buildOneTriggerCriteria(otherSrc, schemaVersion));
        }
        return list;
    }

    // Builds one trigger_criteria entry: source, schema_version, and a 2D conditions array.
    //
    // conditions is a list-of-lists:
    //   - Outer list: OR semantics — event passes if it satisfies ALL conditions in ANY inner list.
    //   - Inner list: AND semantics — all conditions in the group must hold simultaneously.
    // The generator produces 1 or 2 outer groups (50/50); each inner group has 1-3 conditions.
    //
    // Field paths omit the source.version prefix (local names).
    // Supports: ==, !=, >, <, >=, <=, IN, NOT IN (ENUM/INT/LONG), BETWEEN (numeric), BOOLEAN ==, !=.
    private Map<String, Object> buildOneTriggerCriteria(String src, String version) {
        Map<String, Object> trigger = new LinkedHashMap<>();
        trigger.put("source",         src);
        trigger.put("schema_version", version);

        // Decide how many outer groups (1 or 2).
        int outerGroups = random.nextBoolean() ? 1 : 2;
        List<List<Map<String, Object>>> conditions2D = new ArrayList<>();
        for (int g = 0; g < outerGroups; g++) {
            conditions2D.add(buildTriggerConditionGroup(src));
        }

        trigger.put("conditions", conditions2D);
        return trigger;
    }

    // Builds one inner condition group (AND semantics) with 1-3 conditions drawn from the field pool.
    private List<Map<String, Object>> buildTriggerConditionGroup(String src) {
        List<FieldDefinition> pool = new ArrayList<>();
        pool.addAll(staticCatPool(src));
        pool.addAll(dynCatPool(src));
        pool.addAll(staticNumPool(src));
        pool.addAll(staticBoolPool(src));
        pool.addAll(dynBoolPool(src));
        pool.addAll(dynNumPool(src));

        Collections.shuffle(pool, random);
        int conditionCount = 1 + random.nextInt(3); // 1, 2, or 3 per group

        List<Map<String, Object>> group = new ArrayList<>();
        for (int i = 0; i < Math.min(conditionCount, pool.size()); i++) {
            FieldDefinition fd = pool.get(i);
            Map<String, Object> cond = new LinkedHashMap<>();
            cond.put("field", localFieldPath(src, fd));

            if ("ENUM".equals(fd.getConstraintKind())) {
                // STRING: ==, !=, IN, NOT IN
                String[] ops = {"==", "!=", "IN", "NOT IN"};
                String op = ops[random.nextInt(ops.length)];
                cond.put("op", op);
                if ("IN".equals(op) || "NOT IN".equals(op)) {
                    List<String> all = fd.getEnumValues();
                    int count = Math.min(all.size(), 2 + random.nextInt(3));
                    List<String> shuffled = new ArrayList<>(all);
                    Collections.shuffle(shuffled, random);
                    cond.put("value", shuffled.subList(0, count));
                } else {
                    cond.put("value", fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size())));
                }
            } else if ("BOOLEAN".equals(fd.getType())) {
                // BOOLEAN: ==, !=
                cond.put("op",    random.nextBoolean() ? "==" : "!=");
                cond.put("value", random.nextBoolean());
            } else {
                // INT / LONG / FLOAT / DOUBLE
                boolean isStatic    = "static_categorical".equals(fd.getCategory())
                                   || "static_numeric".equals(fd.getCategory());
                boolean isIntOrLong = "INT".equals(fd.getType()) || "LONG".equals(fd.getType());
                String op = pickNumericOp(isStatic, isIntOrLong);
                cond.put("op", op);
                if ("IN".equals(op) || "NOT IN".equals(op)) {
                    int count = 2 + random.nextInt(3);
                    List<Object> vals = new ArrayList<>();
                    for (int k = 0; k < count; k++) vals.add(formatValue(fd, randomInRange(fd)));
                    cond.put("value", vals);
                } else if ("BETWEEN".equals(op)) {
                    cond.put("value", buildBetweenLiterals(fd));
                } else {
                    cond.put("value", formatValue(fd, randomInRange(fd)));
                }
            }
            group.add(cond);
        }
        return group;
    }

    // ── Rule metadata ─────────────────────────────────────────────────────────

    // Builds the metadata block for one rule.
    // event_time - ISO-8601 current time with millisecond precision; user_id - random "user_001".."user_<max>".
    private Map<String, Object> buildRuleMetadata(int maxUserId) {
        int userId = 1 + random.nextInt(Math.max(1, maxUserId));
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("event_time", OffsetDateTime.now().format(TIMESTAMP_FMT));
        meta.put("user_id",    String.format("user_%03d", userId));
        return meta;
    }

    // ── AST node builder ──────────────────────────────────────────────────────

    // Recursively builds one AST node (CONDITION, AND, or OR).
    // Each node independently picks a target source ("A" or "B") for its expressions.
    // ~20% of leaf positions become dynCat+windowAgg AND pairs.
    //
    // CONDITION nodes carry an "is_window" boolean:
    //   true  — expression is a window aggregation (contains "window" + "agg" + "filter" + threshold).
    //   false — plain scalar comparison (no aggregation window involved).
    private Map<String, Object> generateNode(int currentDepth, int maxDepth) {
        String nodeSrc = random.nextBoolean() ? "A" : "B";
        if (currentDepth >= maxDepth || random.nextBoolean()) {
            if (random.nextInt(5) == 0) {
                return buildDynCatPairNode(nodeSrc);
            }
            Object expr = generateExpression(nodeSrc);
            boolean isWindow = (expr instanceof Map<?, ?> m) && m.containsKey("window");
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("type",       "CONDITION");
            node.put("is_window",  isWindow);
            node.put("expression", expr);
            return node;
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("type",     random.nextBoolean() ? "AND" : "OR");
        node.put("children", List.of(
                generateNode(currentDepth + 1, maxDepth),
                generateNode(currentDepth + 1, maxDepth)));
        return node;
    }

    // Picks one of seven expression types at random.
    // Returns String for simple string expressions; Map for structured ones (window, right_field, expr-lhs, etc.).
    private Object generateExpression(String src) {
        return switch (random.nextInt(7)) {
            case 0  -> buildCategoricalExpr(src);
            case 1  -> buildNumericExpr(src);
            case 2  -> buildWindowAggExprMap(src);
            case 3  -> buildBooleanExpr(src);
            case 4  -> buildLinearCombinationExpr(src);
            case 5  -> buildTimestampExpr(src);
            default -> buildExprLhsExpr(src);
        };
    }

    // ── Path helpers ──────────────────────────────────────────────────────────

    // Returns {src}.v2.{name} for flat fields, or {src}.v2.{group}.{name} for nested fields.
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

    // Returns {name} for flat fields, or {group}.{name} for nested fields.
    // Used in trigger_criteria conditions and window filter (no source.version prefix).
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

    private List<FieldDefinition> staticTsPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_STATIC_TIMESTAMP_FIELDS
                               : Constants.SCHEMA_B_STATIC_TIMESTAMP_FIELDS;
    }

    private List<FieldDefinition> dynTsPool(String src) {
        return "A".equals(src) ? Constants.SCHEMA_A_DYNAMIC_TIMESTAMP_FIELDS
                               : Constants.SCHEMA_B_DYNAMIC_TIMESTAMP_FIELDS;
    }

    // ── Expression builders ───────────────────────────────────────────────────

    // Type 0: static categorical (STRING) expression.
    // Ops: ==, !=, IN, NOT IN.
    // For == / != also generates right_field variant (50% chance) when pool has 2+ fields.
    // Examples:
    //   A.v2.loyalty_tier IN ['GOLD', 'PLATINUM']
    //   A.v2.customer_segment NOT IN ['BASIC']
    //   { "field": "A.v2.risk_rating", "op": "!=", "right_field": "A.v2.customer_segment" }
    private Object buildCategoricalExpr(String src) {
        List<FieldDefinition> pool = staticCatPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String[] ops = {"==", "!=", "IN", "NOT IN"};
        String op = ops[random.nextInt(ops.length)];

        if ("IN".equals(op) || "NOT IN".equals(op)) {
            List<String> allVals = fd.getEnumValues();
            int count = Math.min(allVals.size(), 2 + random.nextInt(3));
            List<String> shuffled = new ArrayList<>(allVals);
            Collections.shuffle(shuffled, random);
            String listStr = shuffled.subList(0, count).stream()
                    .map(v -> "'" + v + "'")
                    .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
            return fieldPath(src, fd) + " " + op + " " + listStr;
        }

        // For == / !=: 50% chance right_field comparison when another field is available.
        if (random.nextBoolean()) {
            List<FieldDefinition> others = pool.stream()
                    .filter(f -> !f.getName().equals(fd.getName()))
                    .toList();
            if (!others.isEmpty()) {
                FieldDefinition fd2 = others.get(random.nextInt(others.size()));
                Map<String, Object> expr = new LinkedHashMap<>();
                expr.put("field",       fieldPath(src, fd));
                expr.put("op",          op);
                expr.put("right_field", fieldPath(src, fd2));
                return expr;
            }
        }

        String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
        return fieldPath(src, fd) + " " + op + " '" + val + "'";
    }

    // Type 1: numeric expression for INT, LONG, FLOAT, or DOUBLE fields.
    // Covers all operators supported per type:
    //   INT/LONG  — ==, !=, >, <, >=, <=, BETWEEN, IN, NOT IN
    //   FLOAT/DOUBLE — ==, !=, >, <, >=, <=, BETWEEN (no IN/NOT IN per spec)
    // Static fields allow full op set; dynamic fields allow inequalities only.
    // Returns String for simple literal comparisons; Map for BETWEEN, IN/NOT IN, or right_field.
    private Object buildNumericExpr(String src) {
        boolean useStatic = random.nextBoolean();
        List<FieldDefinition> pool = useStatic ? staticNumPool(src) : dynNumPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));

        boolean isIntOrLong = "INT".equals(fd.getType()) || "LONG".equals(fd.getType());
        // Static allows all comparison ops; dynamic restricts to inequalities.
        String[] compOps = useStatic
                ? new String[]{"==", "!=", ">", "<", ">=", "<="}
                : new String[]{">", "<", ">=", "<="};

        // Variant selection:
        //   0,1 — simple comparison with literal (string expression)
        //   2   — cross-field comparison (right_field, Map)
        //   3   — BETWEEN [lo, hi] (Map)
        //   4   — IN / NOT IN for INT/LONG; BETWEEN fallback for FLOAT/DOUBLE
        int variant = random.nextInt(5);

        if (variant <= 1) {
            String op = compOps[random.nextInt(compOps.length)];
            return fieldPath(src, fd) + " " + op + " " + formatThreshold(fd, randomInRange(fd));
        }

        if (variant == 2) {
            // right_field: any numeric field of any type (numeric types are cross-comparable).
            List<FieldDefinition> allNum = new ArrayList<>(staticNumPool(src));
            allNum.addAll(dynNumPool(src));
            List<FieldDefinition> others = allNum.stream()
                    .filter(f -> !f.getName().equals(fd.getName()))
                    .toList();
            String op = compOps[random.nextInt(compOps.length)];
            if (others.isEmpty()) {
                return fieldPath(src, fd) + " " + op + " " + formatThreshold(fd, randomInRange(fd));
            }
            FieldDefinition fd2 = others.get(random.nextInt(others.size()));
            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field",       fieldPath(src, fd));
            expr.put("op",          op);
            expr.put("right_field", fieldPath(src, fd2));
            return expr;
        }

        if (variant == 3) {
            // BETWEEN — always literal [lo, hi] per spec.
            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field", fieldPath(src, fd));
            expr.put("op",    "BETWEEN");
            expr.put("value", buildBetweenLiterals(fd));
            return expr;
        }

        // variant == 4
        if (isIntOrLong) {
            // IN or NOT IN — literal list; only INT/LONG per spec.
            String op = random.nextBoolean() ? "IN" : "NOT IN";
            int count = 2 + random.nextInt(3);
            List<Object> vals = new ArrayList<>();
            for (int k = 0; k < count; k++) vals.add(formatValue(fd, randomInRange(fd)));
            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field", fieldPath(src, fd));
            expr.put("op",    op);
            expr.put("value", vals);
            return expr;
        }

        // FLOAT/DOUBLE: fall back to BETWEEN since IN/NOT IN are not supported.
        Map<String, Object> expr = new LinkedHashMap<>();
        expr.put("field", fieldPath(src, fd));
        expr.put("op",    "BETWEEN");
        expr.put("value", buildBetweenLiterals(fd));
        return expr;
    }

    // Type 2: window aggregation expression using dynamic numeric fields.
    // Window object: { type, duration } for tumbling; { type, duration, slide } for sliding.
    // "filter" is a LIST of conditions (AND semantics) that must all hold for a historical event
    // to be included in the aggregation. 1-3 conditions are generated per filter list.
    // Example:
    //   { field: "A.v2.daily_spend_total_vnd", agg: "sum",
    //     filter: [
    //       { field: "transaction_type", op: "IN", value: ["TRANSFER", "PAYMENT"] },
    //       { field: "login_channel",    op: "!=", value: "ATM" }
    //     ],
    //     window: { type: "sliding", duration: "20m", slide: "5m" },
    //     op: ">=", threshold: 50000000.00 }
    private Map<String, Object> buildWindowAggExprMap(String src) {
        List<FieldDefinition> pool = dynNumPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));

        String agg     = windowAggs[random.nextInt(windowAggs.length)];
        String winType = windowTypes[random.nextInt(windowTypes.length)];
        String[] ops   = {"<=", ">=", "<", ">"};
        String op      = ops[random.nextInt(ops.length)];

        int    windowMin    = WINDOW_MINUTES[random.nextInt(WINDOW_MINUTES.length)];
        double windowSec    = windowMin * 60.0;
        double eventsPerid  = Math.max(0.1, (double) reqPerSecond * windowSec / idRange);
        double meanValue    = (fd.getMinValue() + fd.getMaxValue()) / 2.0;

        double threshold = switch (agg) {
            case "count" -> eventsPerid * (0.2 + random.nextDouble() * 2.8);
            case "sum"   -> eventsPerid * meanValue * (0.2 + random.nextDouble() * 2.8);
            default      -> randomInRange(fd);
        };

        Map<String, Object> window = new LinkedHashMap<>();
        window.put("type",     winType);
        window.put("duration", windowMin + "m");
        if ("sliding".equals(winType)) {
            window.put("slide", pickSlide(windowMin) + "m");
        }

        Map<String, Object> expr = new LinkedHashMap<>();
        expr.put("field",     fieldPath(src, fd));
        expr.put("agg",       agg);
        expr.put("filter",    buildWindowFilterList(src));   // now a List<Map>
        expr.put("window",    window);
        expr.put("op",        op);
        expr.put("threshold", Double.parseDouble(String.format(Locale.US, "%.2f", threshold)));
        return expr;
    }

    // Type 3: boolean expression using static or dynamic boolean fields.
    // Ops: ==, !=. rhs: literal boolean (70%) or right_field cross-comparison (30%).
    // Examples:
    //   A.v2.is_vip_member == true
    //   { "field": "B.v2.is_2fa_enabled", "op": "==", "right_field": "B.v2.is_suspicious_ip" }
    private Object buildBooleanExpr(String src) {
        boolean useStatic = random.nextBoolean();
        List<FieldDefinition> pool = useStatic ? staticBoolPool(src) : dynBoolPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String op = random.nextBoolean() ? "==" : "!=";

        // 30% chance: right_field comparison with a different boolean field.
        if (random.nextInt(10) < 3) {
            List<FieldDefinition> allBool = new ArrayList<>(staticBoolPool(src));
            allBool.addAll(dynBoolPool(src));
            List<FieldDefinition> others = allBool.stream()
                    .filter(f -> !f.getName().equals(fd.getName()))
                    .toList();
            if (!others.isEmpty()) {
                FieldDefinition fd2 = others.get(random.nextInt(others.size()));
                Map<String, Object> expr = new LinkedHashMap<>();
                expr.put("field",       fieldPath(src, fd));
                expr.put("op",          op);
                expr.put("right_field", fieldPath(src, fd2));
                return expr;
            }
        }

        return fieldPath(src, fd) + " " + op + " " + random.nextBoolean();
    }

    // Type 4: linear combination of two DYNAMIC numeric fields from the same source sharing the SAME range.
    // Random weights w1 and w2 such that w1 + w2 = 1.0 (e.g. 0.65 and 0.35).
    // Raw variant returns a string expression; windowed variant wraps in a window agg Map with "expr" key.
    // Example raw:    (A.v2.daily_spend_total_vnd * 0.65 + A.v2.transfer_amount_today_vnd * 0.35) >= 5000000.00
    // Example windowed:
    //   { expr: "(...)", agg: "sum", filter: {...}, window: { type: "tumbling", duration: "10m" }, op: ">=", threshold: ... }
    private Object buildLinearCombinationExpr(String src) {
        List<FieldDefinition> pool = new ArrayList<>(dynNumPool(src));
        Collections.shuffle(pool, random);

        FieldDefinition fd1 = pool.get(0);
        FieldDefinition fd2 = pool.get(0);

        for (FieldDefinition candidate : pool) {
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

        // Random weights: w1 in [0.05, 0.95], w2 = 1.0 - w1.
        double w1 = (5 + random.nextInt(91)) / 100.0;
        double w2 = Math.round((1.0 - w1) * 100.0) / 100.0;

        String formula = String.format(Locale.US, "(%s * %.2f + %s * %.2f)",
                fieldPath(src, fd1), w1, fieldPath(src, fd2), w2);

        String[] ops = {"<=", ">=", "<", ">"};
        String op = ops[random.nextInt(ops.length)];

        // Windowed variant: use "expr" key so the engine knows the left side is a formula.
        if (random.nextBoolean()) {
            String agg      = windowAggs[random.nextInt(windowAggs.length)];
            String winType  = windowTypes[random.nextInt(windowTypes.length)];
            int windowMin   = WINDOW_MINUTES[random.nextInt(WINDOW_MINUTES.length)];
            double windowSec   = windowMin * 60.0;
            double eventsPerid = Math.max(0.1, (double) reqPerSecond * windowSec / idRange);
            double meanValue   = (fd1.getMinValue() + fd1.getMaxValue()) / 2.0;

            double threshold = switch (agg) {
                case "count" -> eventsPerid * (0.2 + random.nextDouble() * 2.8);
                case "sum"   -> eventsPerid * meanValue * (0.2 + random.nextDouble() * 2.8);
                default      -> randomInRange(fd1);
            };

            Map<String, Object> window = new LinkedHashMap<>();
            window.put("type",     winType);
            window.put("duration", windowMin + "m");
            if ("sliding".equals(winType)) {
                window.put("slide", pickSlide(windowMin) + "m");
            }

            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("expr",      formula);
            expr.put("agg",       agg);
            expr.put("filter",    buildWindowFilterList(src));  // now a List<Map>
            expr.put("window",    window);
            expr.put("op",        op);
            expr.put("threshold", Double.parseDouble(String.format(Locale.US, "%.2f", threshold)));
            return expr;
        }

        // Raw variant: plain string expression.
        double threshold = randomInRange(fd1);
        return formula + " " + op + " " + formatThreshold(fd1, threshold);
    }

    // Type 5: TIMESTAMP field expression.
    // Ops: ==, !=, >, <, >=, <= with ISO-8601 literal or right_field; BETWEEN with [from, to] literals.
    // Examples:
    //   { "field": "A.v2.last_transaction_time", "op": ">", "value": "2026-08-01T00:00:00.000+07:00" }
    //   { "field": "B.v2.last_login_time", "op": "BETWEEN", "value": ["2026-08-01T...", "2026-08-24T..."] }
    //   { "field": "A.v2.account_opened_date", "op": "<", "right_field": "A.v2.last_transaction_time" }
    private Object buildTimestampExpr(String src) {
        List<FieldDefinition> pool = new ArrayList<>(staticTsPool(src));
        pool.addAll(dynTsPool(src));
        if (pool.isEmpty()) {
            return buildBooleanExpr(src); // fallback if schema has no timestamp fields
        }
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String[] compOps = {"==", "!=", ">", "<", ">=", "<="};

        int variant = random.nextInt(3); // 0=BETWEEN, 1=literal, 2=right_field

        if (variant == 0) {
            // BETWEEN: [from, to] ISO-8601 literals, always within the field's epoch range.
            long min   = fd.getMinValue().longValue();
            long max   = fd.getMaxValue().longValue();
            long range = max - min;
            long lo    = min + (long)(range * 0.1 * random.nextDouble());
            long hi    = lo  + (long)((max - lo) * (0.3 + 0.5 * random.nextDouble()));
            hi = Math.min(hi, max);
            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field", fieldPath(src, fd));
            expr.put("op",    "BETWEEN");
            expr.put("value", List.of(epochToIso(lo), epochToIso(hi)));
            return expr;
        }

        if (variant == 1) {
            // ISO-8601 literal value.
            long epoch = (long)(fd.getMinValue() + (fd.getMaxValue() - fd.getMinValue()) * random.nextDouble());
            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field", fieldPath(src, fd));
            expr.put("op",    compOps[random.nextInt(compOps.length)]);
            expr.put("value", epochToIso(epoch));
            return expr;
        }

        // variant == 2: right_field — compare two timestamp fields from the same source.
        List<FieldDefinition> others = pool.stream()
                .filter(f -> !f.getName().equals(fd.getName()))
                .toList();
        if (others.isEmpty()) {
            // Fallback to literal if only one timestamp field exists.
            long epoch = (long)(fd.getMinValue() + (fd.getMaxValue() - fd.getMinValue()) * random.nextDouble());
            Map<String, Object> expr = new LinkedHashMap<>();
            expr.put("field", fieldPath(src, fd));
            expr.put("op",    compOps[random.nextInt(compOps.length)]);
            expr.put("value", epochToIso(epoch));
            return expr;
        }
        FieldDefinition fd2 = others.get(random.nextInt(others.size()));
        Map<String, Object> expr = new LinkedHashMap<>();
        expr.put("field",       fieldPath(src, fd));
        expr.put("op",          compOps[random.nextInt(compOps.length)]);
        expr.put("right_field", fieldPath(src, fd2));
        return expr;
    }

    // Type 6: expression where the left-hand side is an arithmetic expression ("expr" key).
    // Builds: { "expr": "A.v2.f1 OP A.v2.f2", "op": ">=", <rhs> }
    // rhs choices: "value" (literal), "right_field" (single field path), "right_expr" (another expr).
    // Arithmetic ops: +, -, *, / for FLOAT/DOUBLE; +, -, *, %, / for INT/LONG.
    // For / and %: uses an integer constant as the right operand to avoid divide-by-zero.
    // Examples:
    //   { "expr": "A.v2.pages_viewed_session - A.v2.products_viewed_session", "op": ">=", "value": 5 }
    //   { "expr": "A.v2.failed_transactions_count_today + A.v2.online_transfers_today", "op": "<", "right_field": "A.v2.successful_transactions_count_today" }
    //   { "expr": "A.v2.daily_spend_total_vnd * A.v2.current_credit_utilization_pct", "op": ">", "right_expr": "A.v2.monthly_spend_total_vnd * 3" }
    private Map<String, Object> buildExprLhsExpr(String src) {
        List<FieldDefinition> pool = new ArrayList<>(staticNumPool(src));
        pool.addAll(dynNumPool(src));
        Collections.shuffle(pool, random);

        if (pool.size() < 2) {
            return buildWindowAggExprMap(src); // fallback for very small schemas
        }

        FieldDefinition fd1 = pool.get(0);
        FieldDefinition fd2 = pool.get(1);

        boolean isIntType = "INT".equals(fd1.getType()) || "LONG".equals(fd1.getType());
        String[] arithOps = isIntType
                ? new String[]{"+", "-", "*", "%"}
                : new String[]{"+", "-", "*", "/"};
        String arithOp = arithOps[random.nextInt(arithOps.length)];

        // For / and %: use a safe integer constant to avoid divide-by-zero at evaluation time.
        String lhsExpr;
        if ("/".equals(arithOp) || "%".equals(arithOp)) {
            int divisor = 2 + random.nextInt(9);
            lhsExpr = fieldPath(src, fd1) + " " + arithOp + " " + divisor;
        } else {
            lhsExpr = fieldPath(src, fd1) + " " + arithOp + " " + fieldPath(src, fd2);
        }

        String[] compOps = {"==", "!=", ">", "<", ">=", "<="};
        String op = compOps[random.nextInt(compOps.length)];

        Map<String, Object> expr = new LinkedHashMap<>();
        expr.put("expr", lhsExpr);
        expr.put("op",   op);

        // rhs: 0=literal value, 1=right_field, 2=right_expr
        int rhsChoice = random.nextInt(3);
        if (rhsChoice == 0) {
            expr.put("value", formatValue(fd1, randomInRange(fd1)));
        } else if (rhsChoice == 1) {
            List<FieldDefinition> others = pool.stream()
                    .filter(f -> !f.getName().equals(fd1.getName()))
                    .toList();
            if (!others.isEmpty()) {
                expr.put("right_field", fieldPath(src, others.get(random.nextInt(others.size()))));
            } else {
                expr.put("value", formatValue(fd1, randomInRange(fd1)));
            }
        } else {
            // right_expr: third field times a small constant.
            if (pool.size() >= 3) {
                FieldDefinition fd3 = pool.get(2);
                expr.put("right_expr", fieldPath(src, fd3) + " * " + (1 + random.nextInt(5)));
            } else {
                expr.put("value", formatValue(fd1, randomInRange(fd1)));
            }
        }
        return expr;
    }

    // Builds an AND node pairing a dynamic categorical filter with a window aggregation.
    // Used ~20% of the time at leaf positions to create semantically meaningful filter+agg pairs.
    // dynCat ops: ==, !=, IN, NOT IN.
    // Example:
    //   AND[ A.v2.transaction_type NOT IN ['REFUND', 'DEPOSIT'],
    //        { field: A.v2.daily_spend_total_vnd, agg: sum, window: {...}, op: >=, threshold: ... } ]
    private Map<String, Object> buildDynCatPairNode(String src) {
        List<FieldDefinition> pool = dynCatPool(src);
        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String[] ops = {"==", "!=", "IN", "NOT IN"};
        String op = ops[random.nextInt(ops.length)];
        String dynCatExpr;

        if ("IN".equals(op) || "NOT IN".equals(op)) {
            List<String> allVals = fd.getEnumValues();
            int count = Math.min(allVals.size(), 2 + random.nextInt(3));
            List<String> shuffled = new ArrayList<>(allVals);
            Collections.shuffle(shuffled, random);
            String listStr = shuffled.subList(0, count).stream()
                    .map(v -> "'" + v + "'")
                    .collect(java.util.stream.Collectors.joining(", ", "[", "]"));
            dynCatExpr = fieldPath(src, fd) + " " + op + " " + listStr;
        } else {
            String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
            dynCatExpr = fieldPath(src, fd) + " " + op + " '" + val + "'";
        }

        Map<String, Object> dynCatNode = new LinkedHashMap<>();
        dynCatNode.put("type",       "CONDITION");
        dynCatNode.put("is_window",  false);
        dynCatNode.put("expression", dynCatExpr);

        Map<String, Object> windowNode = new LinkedHashMap<>();
        windowNode.put("type",       "CONDITION");
        windowNode.put("is_window",  true);
        windowNode.put("expression", buildWindowAggExprMap(src));

        Map<String, Object> andNode = new LinkedHashMap<>();
        andNode.put("type",     "AND");
        andNode.put("children", List.of(dynCatNode, windowNode));
        return andNode;
    }

    // ── Window helpers ────────────────────────────────────────────────────────

    // Builds the window filter as a LIST of conditions (AND semantics).
    // Each condition uses a LOCAL field path (no source.version prefix) from dynCatPool.
    // 1-3 conditions are generated; all must hold for an event to be included in the aggregation.
    // Ops per condition: ==, !=, IN, NOT IN. Values are literals or literal lists.
    private List<Map<String, Object>> buildWindowFilterList(String src) {
        List<FieldDefinition> pool = new ArrayList<>(dynCatPool(src));
        Collections.shuffle(pool, random);
        int filterCount = 1 + random.nextInt(Math.min(3, pool.size())); // 1, 2, or 3 conditions

        List<Map<String, Object>> filterList = new ArrayList<>();
        for (int i = 0; i < filterCount; i++) {
            FieldDefinition fd = pool.get(i % pool.size());
            String[] ops = {"==", "!=", "IN", "NOT IN"};
            String op = ops[random.nextInt(ops.length)];

            Map<String, Object> cond = new LinkedHashMap<>();
            cond.put("field", localFieldPath(src, fd));
            cond.put("op",    op);

            if ("IN".equals(op) || "NOT IN".equals(op)) {
                List<String> allVals = fd.getEnumValues();
                int count = Math.min(allVals.size(), 2 + random.nextInt(2));
                List<String> shuffled = new ArrayList<>(allVals);
                Collections.shuffle(shuffled, random);
                cond.put("value", shuffled.subList(0, count));
            } else {
                cond.put("value", fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size())));
            }
            filterList.add(cond);
        }
        return filterList;
    }

    // Picks a slide value in minutes that is a factor of windowMin and less than it.
    // Falls back to 1 if no valid factor is found in SLIDE_MINUTES.
    private int pickSlide(int windowMin) {
        List<Integer> valid = new ArrayList<>();
        for (int s : SLIDE_MINUTES) {
            if (s < windowMin && windowMin % s == 0) valid.add(s);
        }
        return valid.isEmpty() ? 1 : valid.get(random.nextInt(valid.size()));
    }

    // Converts Unix epoch seconds to an ISO-8601 string with milliseconds at UTC+7.
    private String epochToIso(long epochSec) {
        return Instant.ofEpochSecond(epochSec)
                .atOffset(ZoneOffset.ofHours(7))
                .format(TIMESTAMP_FMT);
    }

    // ── Op selection helper ───────────────────────────────────────────────────

    // Returns a random numeric operator appropriate for the given field category and type.
    // Static fields allow the full set; dynamic fields restrict to inequalities.
    // isIntOrLong controls whether IN, NOT IN are included in the pool.
    private String pickNumericOp(boolean isStatic, boolean isIntOrLong) {
        if (isStatic && isIntOrLong) {
            String[] ops = {"==", "!=", "<=", ">=", "<", ">", "IN", "NOT IN", "BETWEEN"};
            return ops[random.nextInt(ops.length)];
        } else if (isStatic) {
            String[] ops = {"==", "!=", "<=", ">=", "<", ">", "BETWEEN"};
            return ops[random.nextInt(ops.length)];
        } else if (isIntOrLong) {
            String[] ops = {"<=", ">=", "<", ">", "IN", "NOT IN", "BETWEEN"};
            return ops[random.nextInt(ops.length)];
        } else {
            String[] ops = {"<=", ">=", "<", ">", "BETWEEN"};
            return ops[random.nextInt(ops.length)];
        }
    }

    // ── Value formatting ──────────────────────────────────────────────────────

    // Returns the typed Java value for JSON serialization.
    // INT → int, LONG → long, FLOAT/DOUBLE → double rounded to 2 decimals.
    private Object formatValue(FieldDefinition fd, double value) {
        return switch (fd.getType()) {
            case "INT"    -> (int) Math.round(value);
            case "LONG"   -> Math.round(value);
            case "FLOAT", "DOUBLE" -> Double.parseDouble(String.format(Locale.US, "%.2f", value));
            default        -> String.format(Locale.US, "%.2f", value);
        };
    }

    // Returns the threshold as a String for embedding in expression strings (e.g. "field >= 35").
    // INT/LONG — no decimal; FLOAT/DOUBLE — 2 decimals.
    private String formatThreshold(FieldDefinition fd, double value) {
        if ("INT".equals(fd.getType()) || "LONG".equals(fd.getType())) {
            return String.format(Locale.US, "%.0f", value);
        }
        return String.format(Locale.US, "%.2f", value);
    }

    // Builds a two-element list [lo, hi] for BETWEEN clauses.
    // lo covers the lower 10% of the field range; hi is a random point above lo.
    private List<Object> buildBetweenLiterals(FieldDefinition fd) {
        double lo = fd.getMinValue() + (fd.getMaxValue() - fd.getMinValue()) * 0.1 * random.nextDouble();
        double hi = lo + (fd.getMaxValue() - lo) * (0.3 + 0.5 * random.nextDouble());
        return List.of(formatValue(fd, lo), formatValue(fd, hi));
    }

    private double randomInRange(FieldDefinition fd) {
        return fd.getMinValue() + (fd.getMaxValue() - fd.getMinValue()) * random.nextDouble();
    }
}