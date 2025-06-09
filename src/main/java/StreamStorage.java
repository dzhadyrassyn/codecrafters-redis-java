import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStorage {

    private static final Map<String, Stream> streams = new ConcurrentHashMap<>();

    public static boolean contains(String streamName) {
        return streams.containsKey(streamName);
    }

    public static String add(String streamName, String id, String... values) throws InvalidStreamIdArgumentException {

        Stream stream = streams.computeIfAbsent(streamName, s -> new Stream());;

        StreamId streamId = StreamId.fromString(id);

        StreamId processedId = stream.add(streamId, Helper.parseFieldValuePairs(values));

        synchronized (StreamLocks.getLock(streamName)) {
            StreamLocks.getLock(streamName).notifyAll();
        }

        return processedId.toString();
    }

    public static List<StreamEntry> fetch(String streamName, String from, String to) {

        Stream stream = streams.computeIfAbsent(streamName, s -> new Stream());;

        StreamId streamIdFrom = StreamId.fromString(from);
        StreamId streamIdTo = StreamId.fromString(to);

        return stream.getEntries(streamIdFrom, streamIdTo);
    }

    public static List<StreamEntry> read(String streamName, String from) {

        Stream stream = streams.computeIfAbsent(streamName, s -> new Stream());;
        StreamId streamIdFrom = StreamId.fromString(from);

        return stream.readFromExclusive(streamIdFrom);
    }
}
