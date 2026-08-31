package vdf.vdt.streaming.model.schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class TableSchema implements Serializable {

    private final SourceVersionKey key;
    private final String keyField;
    private final int totalFields;
    private final Map<String, ColumnDefinition> columns; // Key is dot-path (Eg: "debt.transfer_amount_today_vnd")

    public TableSchema(SourceVersionKey key, String keyField, int totalFields, Map<String, ColumnDefinition> columns) {
        this.key = key;
        this.keyField = keyField;
        this.totalFields = totalFields;
        this.columns = columns;
    }

    public SourceVersionKey getKey() { return key; }
    public String getKeyField() { return keyField; }
    public int getTotalFields() { return totalFields; }
    public Map<String, ColumnDefinition> getColumns() { return Collections.unmodifiableMap(columns); }
    public ColumnDefinition getColumn(String fieldPath) { return columns.get(fieldPath); }

}
