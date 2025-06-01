public class StreamId implements Comparable<StreamId> {

    long timestamp;
    long sequence;

    StreamId(long timestamp, long sequence) {
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    StreamId(String id) {

        long[] parts = parse(id);
        this.timestamp = parts[0];
        this.sequence = parts[1];
    }

    static long[] parse(String id) {

        long[] parts = new long[2];
        parts[1] = -1;
        if (id.equals("*")) {
            parts[0] = System.currentTimeMillis();
        } else {
            String[] split = id.split("-");
            parts[0] = Long.parseLong(split[0]);
            if (split.length == 2 && !split[1].equals("*")) {
                parts[1] = Long.parseLong(split[1]);
            }
        }

        return parts;
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
    public String toString() {
        return timestamp + "-" + sequence;
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
}
