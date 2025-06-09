import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamLocks {

    private static final Map<String, Object> streamLocks = new ConcurrentHashMap<>();

    public static Object getLock(String streamName) {
        return streamLocks.computeIfAbsent(streamName, k -> new Object());
    }
}
