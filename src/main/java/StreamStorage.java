import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStorage {

    private static final Map<String, Stream> streams = new ConcurrentHashMap<>();

    public static boolean contains(String streamName) {
        return streams.containsKey(streamName);
    }

    public static String add(String streamName, String... values) throws InvalidStreamIdArgumentException {

        Stream stream = streams.computeIfAbsent(streamName, s -> new Stream());
        String id = values[0];
        validateId(stream.getEntries(), id);

        return stream.add(values);
    }

    private static void validateId(List<StreamEntry> entries, String id) throws InvalidStreamIdArgumentException {

        String[] idAttributes = id.split("-");
        if (id.equals("0-0")) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD must be greater than 0-0"));
        } else if (idAttributes.length != 2) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }

        if (entries.isEmpty()) {
            return;
        }

        StreamEntry entry = entries.getLast();
        String[] lastEntryIdAttributes = entry.id().split("-");
        if (Long.parseLong(idAttributes[0]) < Long.parseLong(lastEntryIdAttributes[0])
                || Long.parseLong(idAttributes[1]) <= Long.parseLong(lastEntryIdAttributes[1])) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }
    }
}
