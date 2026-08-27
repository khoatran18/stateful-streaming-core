package vdf.vdt.streaming.generator.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

// Metadata for a single field in the event schema.
//
// Categories (one per field):
//   static_categorical  - fixed per customer ID (e.g. loyalty_tier)
//   dynamic_categorical - changes each event (e.g. fraud_alert_level)
//   static_numeric      - fixed per customer ID (e.g. age)
//   dynamic_numeric     - real-time metric per event (e.g. current_balance_vnd)
//
// Constraint fields (constraintKind, enumValues, minValue, maxValue) are used internally
// for data generation and rule threshold derivation, but are NOT serialised to JSON.
// The schema payload only exposes name and type; constraint details stay server-side.
//
// Jackson requires a public no-arg constructor and standard getters/setters.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDefinition {

    @JsonProperty("name")
    private String name;

    // "STRING", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP", or "BOOLEAN".
    // STRING pairs with constraintKind ENUM.
    // INT/FLOAT pair with constraintKind RANGE.
    // TIMESTAMP pairs with constraintKind TIMESTAMP; minValue/maxValue are epoch-seconds.
    // BOOLEAN pairs with constraintKind BOOLEAN.
    @JsonProperty("type")
    private String type;

    // "ENUM" or "RANGE" — internal use only, not serialised.
    @JsonIgnore
    private String constraintKind;

    // One of: static_categorical, dynamic_categorical, static_numeric, dynamic_numeric.
    // Runtime-only — not serialised to JSON. In the schema payload the category is already
    // conveyed by the wrapping key (e.g. "static_categorical": [...]).
    private String category;

    // Allowed values when constraintKind == "ENUM" — internal use only, not serialised.
    @JsonIgnore
    private List<String> enumValues;

    // Inclusive lower bound when constraintKind == "RANGE" — internal use only, not serialised.
    @JsonIgnore
    private Double minValue;

    // Inclusive upper bound when constraintKind == "RANGE" — internal use only, not serialised.
    @JsonIgnore
    private Double maxValue;

    public FieldDefinition() {}

    // --- Static factory methods ---

    public static FieldDefinition ofEnum(String name, List<String> enumValues) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "STRING";
        fd.constraintKind = "ENUM";
        fd.enumValues     = List.copyOf(enumValues);
        return fd;
    }

    public static FieldDefinition ofIntRange(String name, int min, int max) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "INT";
        fd.constraintKind = "RANGE";
        fd.minValue       = (double) min;
        fd.maxValue       = (double) max;
        return fd;
    }

    public static FieldDefinition ofFloatRange(String name, double min, double max) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "FLOAT";
        fd.constraintKind = "RANGE";
        fd.minValue       = min;
        fd.maxValue       = max;
        return fd;
    }

    // Creates a LONG field. Semantically equivalent to ofIntRange but uses Java long.
    // Supports the same operators as INT: ==, !=, >, <, >=, <=, BETWEEN, IN, NOT IN.
    public static FieldDefinition ofLongRange(String name, long min, long max) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "LONG";
        fd.constraintKind = "RANGE";
        fd.minValue       = (double) min;
        fd.maxValue       = (double) max;
        return fd;
    }

    // Creates a DOUBLE field. Same precision as FLOAT in JSON output but distinct type.
    // Supports: ==, !=, >, <, >=, <=, BETWEEN (no IN/NOT IN per DATA_TYPE.md).
    public static FieldDefinition ofDoubleRange(String name, double min, double max) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "DOUBLE";
        fd.constraintKind = "RANGE";
        fd.minValue       = min;
        fd.maxValue       = max;
        return fd;
    }

    // Creates a TIMESTAMP field. minEpochSec / maxEpochSec are Unix epoch-seconds
    // (doubles at this scale have no precision loss). DataGenerator emits ISO-8601 strings.
    public static FieldDefinition ofTimestamp(String name, long minEpochSec, long maxEpochSec) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "TIMESTAMP";
        fd.constraintKind = "TIMESTAMP";
        fd.minValue       = (double) minEpochSec;
        fd.maxValue       = (double) maxEpochSec;
        return fd;
    }

    public static FieldDefinition ofBoolean(String name) {
        FieldDefinition fd = new FieldDefinition();
        fd.name           = name;
        fd.type           = "BOOLEAN";
        fd.constraintKind = "BOOLEAN";
        return fd;
    }

    // Stamps the category and returns this for use in stream pipelines:
    //   fields.stream().map(fd -> fd.withCategory("static_categorical")).toList()
    public FieldDefinition withCategory(String category) {
        this.category = category;
        return this;
    }

    // --- Getters & setters (required for Jackson and internal logic) ---

    public String getName()                     { return name; }
    public void   setName(String name)          { this.name = name; }

    public String getType()                     { return type; }
    public void   setType(String type)          { this.type = type; }

    public String getConstraintKind()                       { return constraintKind; }
    public void   setConstraintKind(String constraintKind)  { this.constraintKind = constraintKind; }

    @JsonIgnore
    public String getCategory()                  { return category; }
    public void   setCategory(String category)   { this.category = category; }

    public List<String> getEnumValues()                     { return enumValues; }
    public void         setEnumValues(List<String> vals)    { this.enumValues = vals; }

    public Double getMinValue()                  { return minValue; }
    public void   setMinValue(Double minValue)   { this.minValue = minValue; }

    public Double getMaxValue()                  { return maxValue; }
    public void   setMaxValue(Double maxValue)   { this.maxValue = maxValue; }

    @Override
    public String toString() {
        return "FieldDefinition{name='" + name + "', type='" + type
                + "', constraintKind='" + constraintKind + "', category='" + category + "'}";
    }
}
