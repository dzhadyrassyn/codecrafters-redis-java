import java.time.Instant;
import java.util.*;
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

    private static final Map<String, ValueWithExpiry> data = new HashMap<>();

//    static {
//        startBackgroundCleanup();
//    }

    private static void startBackgroundCleanup() {
        Thread.startVirtualThread(() -> {
            while (true) {
                try {
                    int removed = 0;
                    for (Map.Entry<String, ValueWithExpiry> entry : data.entrySet()) {
                        if (entry.getValue().isExpired()) {
                            data.remove(entry.getKey());
                            removed++;
                        }
                    }
                    if (removed > 0) {
                        System.out.println("Cleaned up " + removed + " expired keys");
                    }

                    // Sleep for 10 seconds
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    System.out.println("Background cleanup thread interrupted");
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                    break;
                }
            }
        });
    }

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

    public static List<String> keys() {
        return new ArrayList<>(data.keySet());
    }

    public static byte[] dumpRDB() {

        String emptyRDBFileContent = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        return Base64.getDecoder().decode(emptyRDBFileContent);
    }

}
