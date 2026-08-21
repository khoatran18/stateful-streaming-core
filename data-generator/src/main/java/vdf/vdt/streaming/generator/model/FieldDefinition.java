package vdf.vdt.streaming.generator.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

// Metadata for a single field in the event schema.
//
// Constraint kinds:
//   ENUM  - value must be one of enum_values (used for all categorical fields)
//   RANGE - value is numeric and must be within [min_value, max_value]
//           INT means integer, FLOAT means floating-point
//
// Categories (one per field):
//   static_categorical  - ENUM, fixed per customer ID (e.g. loyalty_tier)
//   dynamic_categorical - ENUM, changes each event (e.g. fraud_alert_level)
//   static_numeric      - RANGE, fixed per customer ID (e.g. age)
//   dynamic_numeric     - RANGE, real-time metric per event (e.g. current_balance_vnd)
//
// Jackson requires a public no-arg constructor and standard getters/setters.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDefinition {

    @JsonProperty("name")
    private String name;

    // "STRING", "INT", or "FLOAT". STRING always pairs with ENUM, INT/FLOAT with RANGE.
    @JsonProperty("type")
    private String type;

    // "ENUM" or "RANGE"
    @JsonProperty("constraint_kind")
    private String constraintKind;

    // One of: static_categorical, dynamic_categorical, static_numeric, dynamic_numeric.
    // Runtime-only — not serialized to JSON. In the schema payload the category is already
    // conveyed by the wrapping key (e.g. "static_categorical": [...]).
    private String category;

    // Allowed values when constraintKind == "ENUM"
    @JsonProperty("enum_values")
    private List<String> enumValues;

    // Inclusive lower bound when constraintKind == "RANGE"
    @JsonProperty("min_value")
    private Double minValue;

    // Inclusive upper bound when constraintKind == "RANGE"
    @JsonProperty("max_value")
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

    // Stamps the category and returns this for use in stream pipelines:
    //   fields.stream().map(fd -> fd.withCategory("static_categorical")).toList()
    public FieldDefinition withCategory(String category) {
        this.category = category;
        return this;
    }

    // --- Getters & setters (required for Jackson) ---

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
