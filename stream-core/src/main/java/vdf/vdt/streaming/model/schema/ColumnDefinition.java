package vdf.vdt.streaming.model.schema;

import java.io.Serializable;
import java.util.Objects;

public class ColumnDefinition implements Serializable {
    private final String fieldPath;
    private final DataType dataType;
    private final FieldCategory fieldCategory;

    public ColumnDefinition(String fieldPath, DataType dataType, FieldCategory fieldCategory) {
        this.fieldPath = fieldPath;
        this.dataType = dataType;
        this.fieldCategory = fieldCategory;
    }

    public String getFieldPath() { return fieldPath; }
    public DataType getDataType() { return dataType; }
    public FieldCategory getFieldCategory() { return fieldCategory; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnDefinition that)) return false;
        return Objects.equals(fieldCategory, that.fieldCategory) && dataType == that.dataType && fieldCategory == that.fieldCategory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldCategory, fieldPath, dataType);
    }
}
