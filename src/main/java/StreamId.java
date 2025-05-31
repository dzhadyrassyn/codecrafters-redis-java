public class StreamId {

    long timestamp;
    long sequence;

    StreamId(long timestamp, long sequence) {
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    StreamId(String id) {

        if (id.equals("*")) {
            this.timestamp = System.currentTimeMillis();
            this.sequence = -1;
        } else {
            String[] split = id.split("-");
            this.timestamp = Long.parseLong(split[0]);

            if (split[1].equals("*")) {
                this.sequence = -1;
            } else {
                this.sequence = Long.parseLong(split[1]);
            }
        }
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
