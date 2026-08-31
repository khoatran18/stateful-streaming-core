package vdf.vdt.streaming.model.schema;

public record SourceVersionKey(String source, String version) {

    public static SourceVersionKey of(String source, String version) {
        return new SourceVersionKey(source, version);
    }

}
