import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Storage {

    private static class ValueWithExpiry {
        String value;
        Instant expiryTime;

        ValueWithExpiry(String value, Instant expiryTime) {
            this.value = value;
            this.expiryTime = expiryTime;
        }

        boolean isExpired() {
            return expiryTime != null && Instant.now().isAfter(expiryTime);
        }
    }

    private static final Map<String, ValueWithExpiry> data = new ConcurrentHashMap<>();

    public static void set(String key, String value) {
        data.put(key, new ValueWithExpiry(value, null));
    }

    public static void setWithExpiry(String key, String value, long millis) {
        Instant expiry = Instant.now().plusMillis(millis);
        data.put(key, new ValueWithExpiry(value, expiry));
    }

    public static String get(String key) {
        ValueWithExpiry entry = data.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            data.remove(key);
            return null;
        }

        return entry.value;
    }

}
