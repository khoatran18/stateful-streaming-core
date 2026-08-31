package vdf.vdt.streaming.model.schema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

public enum FieldCategory {
    DYNAMIC_NUMERIC("dynamic_numeric"),
    STATIC_CATEGORICAL("static_categorical"),
    DYNAMIC_CATEGORICAL("dynamic_categorical"),
    STATIC_NUMERIC("static_numeric");

    private final String value;

    FieldCategory(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static FieldCategory fromString(String text) {
        for (FieldCategory b : FieldCategory.values()) {
            if (b.value.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new IllegalArgumentException("invalid field category: " + text);
    }

}
