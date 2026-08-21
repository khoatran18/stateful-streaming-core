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
    // At max depth (or randomly earlier) produces a CONDITION leaf, otherwise AND/OR gate.
    private Map<String, Object> generateNode(int currentDepth, int maxDepth) {
        Map<String, Object> node = new LinkedHashMap<>();
        if (currentDepth >= maxDepth || random.nextBoolean()) {
            node.put("type",       "CONDITION");
            node.put("expression", generateExpression());
        } else {
            node.put("type", random.nextBoolean() ? "AND" : "OR");
            node.put("children", List.of(
                    generateNode(currentDepth + 1, maxDepth),
                    generateNode(currentDepth + 1, maxDepth)));
        }
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

    // Type 0: categorical expression using ENUM fields.
    // Static fields are referenced as <field>_current (looks up latest stored state).
    // Operators: == or !=. Value picked from the field's actual enum_values.
    // Examples:
    //   loyalty_tier_current == 'GOLD'
    //   churn_risk_flag != 'CHURNED'
    private String buildCategoricalExpr() {
        boolean useStatic = random.nextBoolean();
        List<FieldDefinition> pool = useStatic
                ? Constants.STATIC_CATEGORICAL_FIELDS
                : Constants.DYNAMIC_CATEGORICAL_FIELDS;

        FieldDefinition fd  = pool.get(random.nextInt(pool.size()));
        String col          = useStatic ? fd.getName() + "_current" : fd.getName();
        String op           = random.nextBoolean() ? "==" : "!=";
        List<String> enums  = fd.getEnumValues();
        String val          = enums.get(random.nextInt(enums.size()));

        return col + " " + op + " '" + val + "'";
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