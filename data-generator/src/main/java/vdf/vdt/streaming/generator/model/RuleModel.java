package vdf.vdt.streaming.generator.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RuleModel {
    @JsonProperty("rule_id")
    private String ruleId;

    @JsonProperty("schema_fields_count")
    private int schemaFieldsCount;

    @JsonProperty("metadata")
    private RuleMetadata metadata;

    @JsonProperty("trigger_criteria")
    private TriggerCriteria triggerCriteria;

    @JsonProperty("condition_tree")
    private RuleNode conditionTree;

    // Getters and Setters
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public int getSchemaFieldsCount() { return schemaFieldsCount; }
    public void setSchemaFieldsCount(int schemaFieldsCount) { this.schemaFieldsCount = schemaFieldsCount; }

    public RuleMetadata getMetadata() { return metadata; }
    public void setMetadata(RuleMetadata metadata) { this.metadata = metadata; }

    public TriggerCriteria getTriggerCriteria() { return triggerCriteria; }
    public void setTriggerCriteria(TriggerCriteria triggerCriteria) { this.triggerCriteria = triggerCriteria; }

    public RuleNode getConditionTree() { return conditionTree; }
    public void setConditionTree(RuleNode conditionTree) { this.conditionTree = conditionTree; }

    // Pre-filter trigger criteria (source, version, list of ANDed condition filters)
    public static class TriggerCriteria {
        @JsonProperty("source")
        private String source;

        @JsonProperty("version")
        private String version;

        @JsonProperty("conditions")
        private List<TriggerCondition> conditions;

        public TriggerCriteria() {}

        public TriggerCriteria(String source, String version, List<TriggerCondition> conditions) {
            this.source = source;
            this.version = version;
            this.conditions = conditions;
        }

        public String getSource() { return source; }
        public void setSource(String source) { this.source = source; }

        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }

        public List<TriggerCondition> getConditions() { return conditions; }
        public void setConditions(List<TriggerCondition> conditions) { this.conditions = conditions; }
    }

    public static class TriggerCondition {
        @JsonProperty("field")
        private String field;

        @JsonProperty("op")
        private String op;

        @JsonProperty("value")
        private Object value;

        public TriggerCondition() {}

        public TriggerCondition(String field, String op, Object value) {
            this.field = field;
            this.op = op;
            this.value = value;
        }

        public String getField() { return field; }
        public void setField(String field) { this.field = field; }

        public String getOp() { return op; }
        public void setOp(String op) { this.op = op; }

        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }
    }

    // Timestamp (ISO-8601) and author of this generated rule.
    public static class RuleMetadata {
        // ISO-8601 timestamp when this rule was generated (e.g. "2026-08-24T09:55:05+07:00").
        @JsonProperty("timestamp")
        private String timestamp;

        // Simulated author ID, randomly drawn from "user_001" to "user_<maxUserId>".
        @JsonProperty("user_id")
        private String userId;

        public RuleMetadata(String timestamp, String userId) {
            this.timestamp = timestamp;
            this.userId    = userId;
        }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
    }

    public static class RuleNode {
        @JsonProperty("type")
        private String type; // "AND", "OR", or "CONDITION"

        // String for categorical / numeric / linear-combination expressions.
        // Map<String, Object> for window aggregation expressions.
        @JsonProperty("expression")
        private Object expression;

        @JsonProperty("children")
        private List<RuleNode> children; // Used when type = "AND" or "OR"

        // Getters and Setters
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Object getExpression() { return expression; }
        public void setExpression(Object expression) { this.expression = expression; }

        public List<RuleNode> getChildren() { return children; }
        public void setChildren(List<RuleNode> children) { this.children = children; }
    }
}