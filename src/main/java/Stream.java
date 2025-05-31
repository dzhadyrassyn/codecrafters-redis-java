import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Stream {

    private final List<StreamEntry> entries = new ArrayList<>();

    public void add(StreamId id, Map<String, String> values) throws InvalidStreamIdArgumentException {

        processAndValidateId(id);

        entries.add(new StreamEntry(id, values));
    }

    public void processAndValidateId(StreamId id) throws InvalidStreamIdArgumentException {

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
}
