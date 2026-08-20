package vdf.vdt.streaming.generator.model;

import java.util.List;

public class SchemaDefinition {
    private int totalFields;
    private List<String> categoricalColumns;
    private List<String> numericColumns;

    public SchemaDefinition(int totalFields, List<String> categoricalColumns, List<String> numericColumns) {
        this.totalFields = totalFields;
        this.categoricalColumns = categoricalColumns;
        this.numericColumns = numericColumns;
    }

    public int getTotalFields() { return totalFields; }
    public void setTotalFields(int totalFields) { this.totalFields = totalFields; }

    public List<String> getCategoricalColumns() { return categoricalColumns; }
    public void setCategoricalColumns(List<String> categoricalColumns) { this.categoricalColumns = categoricalColumns; }

    public List<String> getNumericColumns() { return numericColumns; }
    public void setNumericColumns(List<String> numericColumns) { this.numericColumns = numericColumns; }
}