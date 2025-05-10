import java.util.Arrays;

public class CommandDispatcher {

    private final Config config;

    public CommandDispatcher(Config config) {
        this.config = config;
    }

    public RedisResponse dispatch(String[] args) {
        System.out.println("Processing command: " + Arrays.toString(args));

        if (args == null || args.length == 0) {
            return new TextResponse("-ERR Empty command\r\n");
        }

        String command = args[0].toUpperCase();

        return switch (command) {
            case "PING" -> new TextResponse("+PONG\r\n");
            case "ECHO" -> new TextResponse(Helper.formatBulkString(args[1]));
            case "SET" -> handleSetCommand(args);
            case "GET" -> handleGetCommand(args);
            case "CONFIG" -> handleConfigCommand(args);
            case "KEYS" -> handleKeyCommand();
            case "INFO" -> handleInfoCommand(args);
            case "REPLCONF" -> handleReplConf();
            case "PSYNC" -> handlePSync();
            case "WAIT" -> handleWait();
            default -> unknownCommand(command);
        };
    }

    private RedisResponse handleWait() {

        int replicaCount = ReplicationManager.getReplicaCount();
        return new TextResponse(String.format(":%d\r\n", replicaCount));
    }

    private RedisResponse handleSetCommand(String[] args) {
        System.out.println("Processing SET command");
        String key = args[1];
        String value = args[2];
        Storage.set(key, value);

        if (args.length >= 5 && args[3].equalsIgnoreCase("px")) {
            long ttlMillis = Long.parseLong(args[4]);
            Storage.setWithExpiry(key, value, ttlMillis);
        }

        ReplicationManager.propagateToReplicas(args);
        if (config.isMaster()) {
            return new TextResponse("+OK\r\n");
        }

        return null;
    }

    private RedisResponse handleGetCommand(String[] args) {
        String value = Storage.get(args[1]);
        if (value == null) {
            return new TextResponse("$-1\r\n"); // Null bulk string
        }
        return new TextResponse(Helper.formatBulkString(value));
    }

    private RedisResponse handleConfigCommand(String[] args) {
        if (args.length >= 3 && args[1].equalsIgnoreCase("GET")) {
            String configKey = args[2];
            if (configKey.equals("dir")) {
                return new TextResponse(Helper.formatBulkArray(configKey, config.dir()));
            } else if (configKey.equals("dbfilename")) {
                return new TextResponse(Helper.formatBulkArray(configKey, config.dbfilename()));
            }
        }
        return new TextResponse(Helper.formatBulkArray());
    }

    private RedisResponse handleKeyCommand() {
        return new TextResponse(Helper.formatBulkArray(Storage.keys().toArray(String[]::new)));
    }

    private RedisResponse handleInfoCommand(String[] args) {
        if (args.length < 2 || !args[1].equalsIgnoreCase("replication")) {
            return new TextResponse("+OK\r\n");
        }
        String replicationInfo = """
            # Replication
            role:%s
            master_replid:%s
            master_repl_offset:%s
            """.formatted(
                config.isMaster() ? "master" : "slave",
                Main.MASTER_REPL_ID,
                Main.MASTER_OFFSET
        );
        return new TextResponse(Helper.formatBulkString(replicationInfo));
    }

    private RedisResponse handleReplConf() {
        return new TextResponse("+OK\r\n");
    }

    private RedisResponse handlePSync() {
        return new RDBSyncResponse("+FULLRESYNC " + Main.MASTER_REPL_ID + " " + Main.MASTER_OFFSET + "\r\n", Storage.dumpRDB());
    }

    private RedisResponse unknownCommand(String cmd) {
        return new TextResponse("-ERR unknown command '" + cmd.toUpperCase() + "'\r\n");
    }
}
