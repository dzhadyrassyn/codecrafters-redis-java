public class CommandDispatcher {

    public static String dispatch(String[] args, Config config) {

        String command = args[0].toUpperCase();

        switch (command) {
            case "PING":
                return "+PONG\r\n";
            case "SET" -> {
                String key = args[1];
                String value = args[2];

                Storage.set(key, value);

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
