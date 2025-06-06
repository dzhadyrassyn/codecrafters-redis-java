import java.util.Objects;

public class StreamId implements Comparable<StreamId> {

    private final long timestamp;
    private final long sequence;

    private StreamId(long timestamp, long sequence) {
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    private StreamId(String id) {

        long[] parts = parse(id);
        this.timestamp = parts[0];
        this.sequence = parts[1];
    }

    public static StreamId fromString(String id) {
        return new StreamId(id);
    }

    public static StreamId of(long timestamp, long sequence) {
        return new StreamId(timestamp, sequence);
    }

    static long[] parse(String id) {

        long[] parts = new long[2];
        parts[1] = -1;
        if (id.equals("*") || id.equals("+")) {
            parts[0] = System.currentTimeMillis();
        } else if(id.equals("-")) {
            parts[0] = 0L;
        } else {
            String[] split = id.split("-");
            parts[0] = Long.parseLong(split[0]);
            if (split.length == 2 && !split[1].equals("*")) {
                parts[1] = Long.parseLong(split[1]);
            }
        }

        return parts;
    }

    public long timestamp() {
        return timestamp;
    }

    public long sequence() {
        return sequence;
    }

    public boolean hasSameTimestamp(StreamId other) {
        return this.timestamp == other.timestamp;
    }

    boolean isGreaterThan(StreamId other) {
        return this.compareTo(other) > 0;
    }

    boolean isGreaterOrEqualThan(StreamId other) {
        return this.compareTo(other) >= 0;
    }

    boolean shouldReplaceSequence() {
        return this.sequence == -1;
    }

    @Override
    public int compareTo(StreamId other) {

        int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp != 0) return cmp;
        if (this.sequence == -1 || other.sequence == -1) {
            return 0;
        }
        return Long.compare(this.sequence, other.sequence);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof StreamId other)) return false;
        return timestamp == other.timestamp && sequence == other.sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, sequence);
    }

    @Override
    public String toString() {
        return timestamp + "-" + sequence;
    }
}
