import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStorage {

    private static final Map<String, Stream> streams = new ConcurrentHashMap<>();

    public static boolean contains(String streamName) {
        return streams.containsKey(streamName);
    }

    public static String add(String streamName, String id, String... values) throws InvalidStreamIdArgumentException {

        Stream stream = streams.computeIfAbsent(streamName, s -> new Stream());;

        StreamId streamId = new StreamId(id);


        return stream.add(streamId, Helper.parseFieldValuePairs(values)).toString();
    }
}
