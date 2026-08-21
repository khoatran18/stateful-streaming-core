package vdf.vdt.streaming.generator.rule_gen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import vdf.vdt.streaming.generator.common.Constants;
import vdf.vdt.streaming.generator.model.FieldDefinition;

import java.io.File;
import java.io.IOException;
import java.util.*;

// Generates structured rule definitions (AST condition trees) for the 200-field CDP schema.
//
// Thresholds and enum values in expressions are derived from FieldDefinition metadata,
// so generated conditions are domain-meaningful (e.g. loyalty_tier_current == 'GOLD').
//
// Window threshold formula:
//   expectedEventsPerId = reqPerSecond * windowSeconds / idRange
//   count threshold     = expectedEventsPerId * U[0.2, 3.0]
//   sum threshold       = expectedEventsPerId * meanValue * U[0.2, 3.0]
//   min/max/avg         = random value within [field.minValue, field.maxValue]
//
// Window expression format: <field>_<winType>_<agg>_<windowTime>_<subIntervalTime>
//   tumbling: bucket time must evenly divide window time (windowMinutes % bucketMinutes == 0)
//   sliding:  slide time must be strictly less than window time
//   Both sub-interval times are in whole minutes and always < window time.
//
// Field naming rules:
//   static fields (categorical + numeric) → always referenced with "_current" suffix
//   dynamic numeric fields                → no suffix; referenced by raw field name
//   dynamic categorical fields            → MUST be paired with a window agg in an AND node;
//                                          never appear as a standalone CONDITION leaf
public class RuleGenerator {

    // Window sizes in minutes (2m–30m). Sub-interval (bucket/slide) is derived per window.
    private static final int[] WINDOW_MINUTES = {2, 5, 10, 15, 20, 25, 30};
    private final String[] windowAggs   = {"sum", "count", "avg", "max", "min"};
    // tumbling = fixed non-overlapping intervals, sliding = overlapping (step < window size)
    private final String[] windowTypes  = {"tumbling", "sliding"};
    private final Random random = new Random();

    private final int reqPerSecond;
    private final int idRange;

    public RuleGenerator(int idRange, int reqPerSecond) {
        this.idRange      = Math.max(1, idRange);
        this.reqPerSecond = reqPerSecond;
    }

    // Generates totalRules rules and writes them as a JSON array to filePath.
    public void generateRulesToFile(int totalRules, String filePath) throws IOException {
        List<Map<String, Object>> allRules = new ArrayList<>(totalRules);

        for (int i = 0; i < totalRules; i++) {
            Map<String, Object> rule = new LinkedHashMap<>();
            rule.put("rule_id",             "rule_200_" + i);
            rule.put("schema_fields_count", Constants.TOTAL_FIELDS);
            rule.put("condition_tree",      generateNode(1, random.nextInt(4) + 2));
            allRules.add(rule);
        }

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(new File(filePath), allRules);
        System.out.println("Successfully generated " + totalRules + " rules into: " + filePath);
    }

    // Recursively builds one AST node. Depth is randomised between 2 and 5 total levels.
    // At leaf positions (~20% chance) a dynamic-categorical+window AND pair is injected instead
    // of a simple CONDITION, ensuring dynCat expressions are never standalone.
    private Map<String, Object> generateNode(int currentDepth, int maxDepth) {
        if (currentDepth >= maxDepth || random.nextBoolean()) {
            // ~20% of leaf positions: dynCat paired with window agg in an AND node.
            // This keeps dynamic categorical expressions meaningful (filter + aggregation).
            if (random.nextInt(5) == 0) {
                return buildDynCatPairNode();
            }
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("type",       "CONDITION");
            node.put("expression", generateExpression());
            return node;
        }
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("type", random.nextBoolean() ? "AND" : "OR");
        node.put("children", List.of(
                generateNode(currentDepth + 1, maxDepth),
                generateNode(currentDepth + 1, maxDepth)));
        return node;
    }

    // Picks one of four expression types at random.
    private String generateExpression() {
        return switch (random.nextInt(4)) {
            case 0  -> buildCategoricalExpr();
            case 1  -> buildRawNumericExpr();
            case 2  -> buildWindowAggExpr();
            default -> buildLinearCombinationExpr();
        };
    }

    // Type 0: static categorical expression.
    // Always uses static categorical fields — these are fixed per customer and need no pairing.
    // Referenced with "_current" suffix (looks up latest stored state).
    // Operators: == or !=. Value picked from the field's actual enum_values.
    // Dynamic categorical is excluded here; see buildDynCatPairNode for its handling.
    // Examples:
    //   loyalty_tier_current == 'GOLD'
    //   risk_rating_current != 'VERY_HIGH'
    private String buildCategoricalExpr() {
        FieldDefinition fd = Constants.STATIC_CATEGORICAL_FIELDS
                .get(random.nextInt(Constants.STATIC_CATEGORICAL_FIELDS.size()));
        String op  = random.nextBoolean() ? "==" : "!=";
        String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
        return fd.getName() + "_current" + " " + op + " '" + val + "'";
    }

    // Type 1: raw numeric expression using RANGE fields.
    // Static fields referenced with _current suffix.
    // Static fields allow full operator set (==, !=, <=, >=, <, >).
    // Dynamic fields allow inequalities only (<=, >=, <, >).
    // Threshold is a random value within [field.minValue, field.maxValue].
    // Examples:
    //   age_current >= 35
    //   daily_spend_total_vnd > 50000000.00
    private String buildRawNumericExpr() {
        boolean useStatic = random.nextBoolean();
        List<FieldDefinition> pool = useStatic
                ? Constants.STATIC_NUMERIC_FIELDS
                : Constants.DYNAMIC_NUMERIC_FIELDS;

        FieldDefinition fd = pool.get(random.nextInt(pool.size()));
        String col         = useStatic ? fd.getName() + "_current" : fd.getName();

        String[] ops = useStatic
                ? new String[]{"==", "!=", "<=", ">=", "<", ">"}
                : new String[]{"<=", ">=", "<", ">"};
        String op = ops[random.nextInt(ops.length)];

        double threshold = randomInRange(fd);

        return col + " " + op + " " + formatThreshold(fd, threshold);
    }

    // Type 2: window aggregation expression using dynamic numeric fields only.
    // Expression name format: <field>_<windowType>_<agg>_<windowTime>_<subIntervalTime>
    //   windowTime    = window duration in minutes (2m–30m)
    //   subIntervalTime = bucket (tumbling) or slide step (sliding), in whole minutes, < windowTime
    //   tumbling constraint: windowMinutes % bucketMinutes == 0
    // Threshold is derived from expected event count in the window per customer ID.
    // Examples:
    //   daily_spend_total_vnd_tumbling_sum_10m_2m >= 50000000.00
    //   fraud_probability_score_sliding_count_5m_1m > 3.00
    private String buildWindowAggExpr() {
        FieldDefinition fd = Constants.DYNAMIC_NUMERIC_FIELDS
                .get(random.nextInt(Constants.DYNAMIC_NUMERIC_FIELDS.size()));

        String agg     = windowAggs[random.nextInt(windowAggs.length)];
        String winType = windowTypes[random.nextInt(windowTypes.length)];
        String[] ops   = {"<=", ">=", "<", ">"};
        String op      = ops[random.nextInt(ops.length)];

        int windowMin      = WINDOW_MINUTES[random.nextInt(WINDOW_MINUTES.length)];
        int subIntervalMin = pickSubInterval(winType, windowMin);

        String windowLabel      = windowMin + "m";
        String subIntervalLabel = subIntervalMin + "m";

        double windowSeconds       = windowMin * 60.0;
        double expectedEventsPerId = Math.max(0.1, (double) reqPerSecond * windowSeconds / idRange);
        double meanValue           = (fd.getMinValue() + fd.getMaxValue()) / 2.0;

        double threshold = switch (agg) {
            case "count" -> expectedEventsPerId * (0.2 + random.nextDouble() * 2.8);
            case "sum"   -> expectedEventsPerId * meanValue * (0.2 + random.nextDouble() * 2.8);
            default      -> randomInRange(fd); // min, max, avg -> value within field range
        };

        String fieldExpr = fd.getName() + "_" + winType + "_" + agg + "_" + windowLabel + "_" + subIntervalLabel;
        return fieldExpr + " " + op + " " + String.format(Locale.US, "%.2f", threshold);
    }

    // Picks a valid sub-interval (bucket for tumbling, slide step for sliding) in whole minutes.
    // For tumbling: candidate minutes must evenly divide windowMin; picks one at random.
    // For sliding:  any minute value in [1, windowMin - 1]; picks one at random.
    private int pickSubInterval(String winType, int windowMin) {
        if ("tumbling".equals(winType)) {
            // Collect all divisors of windowMin that are strictly less than windowMin.
            List<Integer> divisors = new ArrayList<>();
            for (int m = 1; m < windowMin; m++) {
                if (windowMin % m == 0) divisors.add(m);
            }
            return divisors.get(random.nextInt(divisors.size()));
        } else {
            // Sliding: any whole-minute step strictly less than window size.
            return 1 + random.nextInt(windowMin - 1); // [1, windowMin - 1]
        }
    }

    // Builds an AND node pairing a dynamic categorical filter with a window aggregation.
    // Dynamic categorical fields (e.g. transaction_type, card_status) represent event-level
    // attributes that only make sense as a filter scoping a computation, not as a standalone
    // condition. The AND structure is: (dynCat == value) AND (windowAgg op threshold).
    // This mirrors real-world rules like "count transfers in 10m with transaction_type == TRANSFER".
    // Operators for dynCat: == or != only (categorical matching, no ordering).
    private Map<String, Object> buildDynCatPairNode() {
        // Dynamic categorical CONDITION child.
        FieldDefinition fd  = Constants.DYNAMIC_CATEGORICAL_FIELDS
                .get(random.nextInt(Constants.DYNAMIC_CATEGORICAL_FIELDS.size()));
        String op  = random.nextBoolean() ? "==" : "!=";
        String val = fd.getEnumValues().get(random.nextInt(fd.getEnumValues().size()));
        String dynCatExpr = fd.getName() + " " + op + " '" + val + "'";

        Map<String, Object> dynCatNode = new LinkedHashMap<>();
        dynCatNode.put("type",       "CONDITION");
        dynCatNode.put("expression", dynCatExpr);

        // Window aggregation CONDITION child — the meaningful computation over filtered events.
        Map<String, Object> windowNode = new LinkedHashMap<>();
        windowNode.put("type",       "CONDITION");
        windowNode.put("expression", buildWindowAggExpr());

        Map<String, Object> andNode = new LinkedHashMap<>();
        andNode.put("type",     "AND");
        andNode.put("children", List.of(dynCatNode, windowNode));
        return andNode;
    }

    // Type 3: linear combination of two numeric fields (dynamic or static).
    // Formula: (field1 * 0.7 + field2 * 0.3) op threshold
    // Threshold is random within the combined [min, max] range of both fields.
    // Example:
    //   (current_balance_vnd * 0.7 + monthly_spend_total_vnd * 0.3) >= 500000000.00
    private String buildLinearCombinationExpr() {
        List<FieldDefinition> all = new ArrayList<>(Constants.DYNAMIC_NUMERIC_FIELDS);
        all.addAll(Constants.STATIC_NUMERIC_FIELDS);

        FieldDefinition fd1 = all.get(random.nextInt(all.size()));
        FieldDefinition fd2 = all.get(random.nextInt(all.size()));

        String[] ops = {"<=", ">=", "<", ">"};
        String op = ops[random.nextInt(ops.length)];

        double maxOfTwo = Math.max(fd1.getMaxValue(), fd2.getMaxValue());
        double minOfTwo = Math.min(fd1.getMinValue(), fd2.getMinValue());
        double threshold = minOfTwo + (maxOfTwo - minOfTwo) * random.nextDouble();

        return "(" + fd1.getName() + " * 0.7 + " + fd2.getName() + " * 0.3) "
                + op + " " + String.format(Locale.US, "%.2f", threshold);
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

    private double parseWindowToSeconds(String time) {
        if (time.endsWith("s")) return Double.parseDouble(time.replace("s", ""));
        if (time.endsWith("m")) return Double.parseDouble(time.replace("m", "")) * 60;
        if (time.endsWith("h")) return Double.parseDouble(time.replace("h", "")) * 3600;
        return 60.0; // default: 1 minute
    }
}