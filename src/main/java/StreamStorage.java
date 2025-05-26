import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStorage {

    private static class StreamEntry {
        String id;
        Map<String, String> values;
    }

    private static final Map<String, List<StreamEntry>> data = new ConcurrentHashMap<>();

    public static boolean contains(String streamName) {
        return data.containsKey(streamName);
    }

    public static String add(String streamName, String... values) {

        List<StreamEntry> streamEntries = data.getOrDefault(streamName, new ArrayList<>());
        int i = 0;
        String id = values[i++];
        Map<String, String> streamValues = new ConcurrentHashMap<>();
        while (i < values.length) {
            String key = values[i++];
            String value = values[i++];
            streamValues.put(key, value);
        }
        StreamEntry streamEntry = new StreamEntry();
        streamEntry.id = id;
        streamEntry.values = streamValues;
        streamEntries.add(streamEntry);
        data.put(streamName, streamEntries);

        return id;
    }
}
