public class CommandDispatcher {

    public static String dispatch(String[] args, Config config) {

        String command = args[0].toUpperCase();

        switch (command) {
            case "PING":
                return "+PONG\r\n";
            case "SET" -> {
                if (args.length == 5 && args[3].equalsIgnoreCase("px")) {
                    long ttlMs = Long.parseLong(args[4]);
                    Storage.setWithExpiry(args[1], args[2], ttlMs);
                } else {
                    Storage.set(args[1], args[2]);
                }
                ReplicationManager.propagateToReplicas(args);
                return "+OK\r\n";
            }
            case "GET" -> {
                String key = args[1];
                String value = Storage.get(key);
                if (value == null) return "$-1\r\n";
                return "$" + value.length() + "\r\n" + value + "\r\n";
            }
            default -> {
                return "-ERR Unknown command\r\n";
            }
        }
    }
}
