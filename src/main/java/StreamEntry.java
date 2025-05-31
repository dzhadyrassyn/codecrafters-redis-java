import java.util.Map;

public record StreamEntry(StreamId id, Map<String, String> values) {}