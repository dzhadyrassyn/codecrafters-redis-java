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

        String processedId = processId(stream.getEntries(), id);

        validateId(stream.getEntries(), processedId);

        return stream.add(processedId, values);
    }

    private static String processId(List<StreamEntry> entries, String id) {

        String[] idAttributes = id.split("-");

        if (idAttributes[1].equals("*")) {
            return replaceSequencePart(entries, idAttributes);
        }

        return idAttributes[0] + "-" + idAttributes[1];
    }

    private static String replaceSequencePart(List<StreamEntry> entries, String[] idAttributes) {

        if (idAttributes[0].equals("0")) {
            return "0-1";
        }

        if (entries.isEmpty()) {
            return idAttributes[0] + "-0";
        }

        StreamEntry entry = entries.getLast();
        String[] lastEntryIdAttributes = entry.id().split("-");
        if (idAttributes[0].equals(lastEntryIdAttributes[0])) {
            return idAttributes[0] + "-" + (Long.parseLong(lastEntryIdAttributes[1]) + 1);
        }

        return idAttributes[0] + "-0";
    }

    private static void validateId(List<StreamEntry> entries, String id) throws InvalidStreamIdArgumentException {

        String[] idAttributes = id.split("-");

        if (idAttributes.length != 2) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        } else if (idAttributes[0].equals("0") && idAttributes[1].equals("0")) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD must be greater than 0-0"));
        }

        if (entries.isEmpty()) {
            return;
        }

        StreamEntry entry = entries.getLast();
        String[] lastEntryIdAttributes = entry.id().split("-");
        long idTime = Long.parseLong(idAttributes[0]);
        long idSeq = Long.parseLong(idAttributes[1]);
        long lastTime = Long.parseLong(lastEntryIdAttributes[0]);
        long lastSeq = Long.parseLong(lastEntryIdAttributes[1]);

        if (idTime < lastTime || (idTime == lastTime && idSeq <= lastSeq)) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }
    }
}
