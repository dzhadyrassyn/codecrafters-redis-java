public class StreamId {

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

    long[] parse(String id) {

        long[] parts = new long[2];
        parts[1] = -1;
        if (id.equals("*")) {
            parts[0] = System.currentTimeMillis();
        } else {
            String[] split = id.split("-");
            parts[0] = Long.parseLong(split[0]);

            if (!split[1].equals("*")) {
                parts[1] = Long.parseLong(split[1]);
            }
        }

        return parts;
    }

    boolean isGreaterThan(StreamId streamId) {
        return this.timestamp > streamId.timestamp || (this.timestamp == streamId.timestamp && this.sequence > streamId.sequence);
    }

    boolean shouldReplaceSequence() {
        return this.sequence == -1;
    }

    @Override
    public String toString() {
        return timestamp + "-" + sequence;
    }
}
