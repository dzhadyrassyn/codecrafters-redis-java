import java.util.*;

public class Stream {

    private final List<StreamEntry> entries = Collections.synchronizedList(new ArrayList<>());

    public StreamId add(StreamId id, Map<String, String> values) throws InvalidStreamIdArgumentException {

        StreamId processedId = processAndValidateId(id);

        entries.add(new StreamEntry(processedId, values));

        return processedId;
    }

    private StreamId processAndValidateId(StreamId id) throws InvalidStreamIdArgumentException {

        if (id.shouldReplaceSequence()) {
            id = replaceSequencePart(id);
        }

        validateId(id);

        return id;
    }

    private StreamId replaceSequencePart(StreamId id) {

        Optional<StreamEntry> lastEntryOptional = getLast();

        if (id.timestamp() == 0) {
            return StreamId.of(id.timestamp(), 1L);
        }

        if (lastEntryOptional.isEmpty()) {
            return StreamId.of(id.timestamp(), 0L);
        }

        StreamId lastEntryId = lastEntryOptional.get().id();
        if (id.hasSameTimestamp(lastEntryId)) {
            return StreamId.of(id.timestamp(), lastEntryId.sequence() + 1);
        }

        return StreamId.of(id.timestamp(), 0L);
    }

    private void validateId(StreamId id) throws InvalidStreamIdArgumentException {

        Optional<StreamEntry> lastEntryOptional = getLast();

        if (id.timestamp() == 0 && id.sequence() == 0) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD must be greater than 0-0"));
        }

        if (lastEntryOptional.isEmpty()) {
            return;
        }

        StreamId lastEntryId = lastEntryOptional.get().id();
        if (!id.isGreaterThan(lastEntryId)) {
            throw new InvalidStreamIdArgumentException(Helper.formatSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }
    }

    public Optional<StreamEntry> getLast() {

        if (entries.isEmpty()) {
            return Optional.empty();
        }

        return Optional.ofNullable(entries.getLast());
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

    public List<StreamEntry> readFromExclusive(StreamId streamIdFrom) {

        int i = 0;
        while (i < entries.size()) {
            StreamEntry entry = entries.get(i);
            if (entry.id().isGreaterThan(streamIdFrom)) {
                break;
            }
            ++i;
        }

        if (i == entries.size()) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList(entries.subList(i, entries.size()));
    }
}
