import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStorage {

    private static final Map<String, Stream> streams = new ConcurrentHashMap<>();

    public static boolean contains(String streamName) {
        return streams.containsKey(streamName);
    }

    public static String add(String streamName, String... values) {

        Stream stream = streams.computeIfAbsent(streamName, s -> new Stream());
        return stream.add(values);
    }
}
