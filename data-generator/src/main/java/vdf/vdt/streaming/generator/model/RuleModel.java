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

    @JsonProperty("condition_tree")
    private RuleNode conditionTree;

    // Getters and Setters
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }

    public int getSchemaFieldsCount() { return schemaFieldsCount; }
    public void setSchemaFieldsCount(int schemaFieldsCount) { this.schemaFieldsCount = schemaFieldsCount; }

    public RuleNode getConditionTree() { return conditionTree; }
    public void setConditionTree(RuleNode conditionTree) { this.conditionTree = conditionTree; }

    public static class RuleNode {
        @JsonProperty("type")
        private String type; // "AND", "OR", or "CONDITION"

        @JsonProperty("expression")
        private String expression; // Used when type = "CONDITION"

        @JsonProperty("children")
        private List<RuleNode> children; // Used when type = "AND" hoặc "OR"

        // Getters and Setters
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getExpression() { return expression; }
        public void setExpression(String expression) { this.expression = expression; }

        public List<RuleNode> getChildren() { return children; }
        public void setChildren(List<RuleNode> children) { this.children = children; }
    }
}