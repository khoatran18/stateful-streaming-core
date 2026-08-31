package vdf.vdt.streaming.model.event;

import java.io.Serializable;

public class RawKafkaDataEvent implements Serializable {
    private String source;
    private String version;
    private byte[] payload;

    public RawKafkaDataEvent() {}

    public RawKafkaDataEvent(String source, String version, byte[] payload) {
        this.source = source;
        this.version = version;
        this.payload = payload;
    }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
}
