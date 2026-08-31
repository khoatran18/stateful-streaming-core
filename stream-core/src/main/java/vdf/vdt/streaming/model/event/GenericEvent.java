package vdf.vdt.streaming.model.event;

import vdf.vdt.streaming.model.schema.SourceVersionKey;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class GenericEvent implements Serializable {
    private String customerId;
    private Instant eventTime;
    private SourceVersionKey sourceVersionKey;
    private HashMap<String, Object> fields;

    public GenericEvent() {
        this.fields = new HashMap<>();
    }

    public GenericEvent(String customerId, Instant eventTime, SourceVersionKey sourceVersionKey, Map<String, Object> fields) {
        this.customerId = customerId;
        this.eventTime = eventTime;
        this.sourceVersionKey = sourceVersionKey;
        this.fields = fields != null ? new HashMap<>(fields) : new HashMap<>();
    }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public Instant getEventTime() { return eventTime; }
    public void setEventTime(Instant eventTime) { this.eventTime = eventTime; }

    public SourceVersionKey getSourceVersionKey() { return sourceVersionKey; }
    public void setSourceVersionKey(SourceVersionKey sourceVersionKey) { this.sourceVersionKey = sourceVersionKey; }

    public HashMap<String, Object> getFields() { return fields; }
    public void setFields(HashMap<String, Object> fields) { this.fields = fields; }

    public Object getField(String fieldPath) {
        return fields != null ? fields.get(fieldPath) : null;
    }

    @Override
    public String toString() {
        return "GenericEvent{" +
                "customerId='" + customerId + '\'' +
                ", eventTime=" + eventTime +
                ", key=" + sourceVersionKey +
                ", fieldsCount=" + (fields != null ? fields.size() : 0) +
                ", sampleFields=" + fields +
                '}';
    }
}