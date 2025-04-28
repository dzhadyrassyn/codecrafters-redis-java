public class CommandDispatcher {

    private final Config config;

    public CommandDispatcher(Config config) {
        this.config = config;
    }

    public RedisResponse dispatch(String[] args) {
        if (args == null || args.length == 0) {
            return new TextResponse("-ERR Empty command\r\n");
        }

        String command = args[0].toUpperCase();

        return switch (command) {
            case "PING" -> new TextResponse("+PONG\r\n");
            case "ECHO" -> new TextResponse(formatBulkString(args[1]));
            case "SET" -> handleSetCommand(args);
            case "GET" -> handleGetCommand(args);
            case "CONFIG" -> handleConfigCommand(args);
            case "KEYS" -> handleKeyCommand();
            case "INFO" -> handleInfoCommand(args);
            case "REPLCONF" -> handleReplConf();
            case "PSYNC" -> handlePSync();
            default -> unknownCommand(command);
        };
    }

    private RedisResponse handleSetCommand(String[] args) {
        String key = args[1];
        String value = args[2];
        Storage.set(key, value);

        if (args.length >= 5 && args[3].equalsIgnoreCase("px")) {
            long ttlMillis = Long.parseLong(args[4]);
            Storage.expire(key, ttlMillis);
        }

        ReplicationManager.propagateToReplicas(args);
        return new TextResponse("+OK\r\n");
    }

    private RedisResponse handleGetCommand(String[] args) {
        String value = Storage.get(args[1]);
        if (value == null) {
            return new TextResponse("$-1\r\n"); // Null bulk string
        }
        return new TextResponse(formatBulkString(value));
    }

    private RedisResponse handleConfigCommand(String[] args) {
        if (args.length >= 3 && args[1].equalsIgnoreCase("GET")) {
            String configKey = args[2];
            if (configKey.equals("dir")) {
                return new TextResponse(formatBulkArray(List.of(configKey, config.rdbDir())));
            } else if (configKey.equals("dbfilename")) {
                return new TextResponse(formatBulkArray(List.of(configKey, config.rdbFileName())));
            }
        }
        return new TextResponse(formatBulkArray(List.of()));
    }

    private RedisResponse handleKeyCommand() {
        return new TextResponse(formatBulkArray(Storage.keys()));
    }

    private RedisResponse handleInfoCommand(String[] args) {
        if (args.length < 2 || !args[1].equalsIgnoreCase("replication")) {
            return new TextResponse("+OK\r\n");
        }
        String replicationInfo = """
            # Replication
            role:%s
            master_replid:%s
            master_repl_offset:%d
            """.formatted(
                config.isMaster() ? "master" : "slave",
                Main.master_replid,
                Main.replicationOffset
        );
        return new TextResponse(formatBulkString(replicationInfo));
    }

    private RedisResponse handleReplConf() {
        return new TextResponse("+OK\r\n");
    }

    private RedisResponse handlePSync() {
        return new RDBSyncResponse("+FULLRESYNC " + Main.master_replid + " 0\r\n", Storage.dumpRDB());
    }

    private RedisResponse unknownCommand(String cmd) {
        return new TextResponse("-ERR unknown command '" + cmd.toUpperCase() + "'\r\n");
    }

    // Utility formatting methods
    private static String formatBulkString(String value) {
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    private static String formatBulkArray(List<String> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(items.size()).append("\r\n");
        for (String item : items) {
            sb.append("$").append(item.length()).append("\r\n");
            sb.append(item).append("\r\n");
        }
        return sb.toString();
    }
}
