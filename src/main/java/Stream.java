import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class Stream {

    private final List<StreamEntry> entries = new CopyOnWriteArrayList<>();

    public void add(StreamId id, Map<String, String> values) throws InvalidStreamIdArgumentException {

        processAndValidateId(id);

        entries.add(new StreamEntry(id, values));
    }

    private void processAndValidateId(StreamId id) throws InvalidStreamIdArgumentException {

        if (id.shouldReplaceSequence()) {
            replaceSequencePart(id);
        }

        validateId(id);
    }

    private void replaceSequencePart(StreamId id) {

        List<StreamEntry> entries = getEntries();

        if (id.timestamp == 0) {
            id.sequence = 1;
            return;
        }

        if (entries.isEmpty()) {
            id.sequence = 0;
            return;
        }

        StreamEntry entry = entries.getLast();

        StreamId lastEntry = entry.id();
        if (id.timestamp == lastEntry.timestamp) {
            id.sequence = lastEntry.sequence + 1;
            return;
        }

        id.sequence = 0;
    }

    private void validateId(StreamId id) throws InvalidStreamIdArgumentException {

        List<StreamEntry> entries = getEntries();

        if (id.timestamp == 0 && id.sequence == 0) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD must be greater than 0-0"));
        }

        if (entries.isEmpty()) {
            return;
        }

        StreamEntry entry = entries.getLast();
        StreamId lastEntry = entry.id();
        if (!id.isGreaterThan(lastEntry)) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }
    }

    public List<StreamEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    public List<StreamEntry> getEntries(StreamId from, StreamId to) {

        int left = 0, right = entries.size() - 1;
        while (left <= right && !entries.get(left).id().isGreaterOrEqualThan(from)) {
            ++left;
        }

        while (left <= right && entries.get(right).id().isGreaterThan(to)) {
            --right;
        }

        if (left > right) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList(entries.subList(left, right + 1));
    }
}
