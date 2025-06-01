import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommandDispatcher {

    private final Config config;

    public CommandDispatcher(Config config) {
        this.config = config;
    }

    public RedisResponse dispatch(String[] args, long ack) {

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
            case "REPLCONF" -> handleReplConf(args, ack);
            case "PSYNC" -> handlePSync();
            case "WAIT" -> handleWait(args);
            case "TYPE" -> handleType(args);
            case "XADD" -> handleXAdd(args);
            case "XRANGE" -> handleXRange(args);
            default -> unknownCommand(command);
        };
    }

    private RedisResponse handleXRange(String[] args) {

        String streamName = args[1];
        String from = args[2];
        String to = args[3];

        List<StreamEntry> data = StreamStorage.fetch(streamName, from, to);

        return new TextResponse(Helper.formatXRange(data));
    }

    private RedisResponse handleXAdd(String[] args) {

        String streamName = args[1];
        String[] streamArgs = Arrays.copyOfRange(args, 3, args.length);

        String id = args[2];
        try {
            id = StreamStorage.add(streamName, id, streamArgs);
        } catch (InvalidStreamIdArgumentException e) {
            return new TextResponse(e.getMessage());
        }
        return new TextResponse(Helper.formatBulkString(id));
    }

    private RedisResponse handleType(String[] args) {

        String key = args[1];
        String value = Storage.get(key);
        if (value != null) {
            return new TextResponse(Helper.formatType("string"));
        }

        boolean isStream = StreamStorage.contains(key);
        if (isStream) {
            return new TextResponse(Helper.formatType("stream"));
        }

        return new TextResponse(Helper.formatType("none"));
    }

    private RedisResponse handleWait(String[] args) {

        long expectedReplicas = Long.parseLong(args[1]);
        if (expectedReplicas == 0) {
            return new TextResponse(Helper.formatCount(0));
        }
        long expireTime = Long.parseLong(args[2]);

        ReplicationManager.sendGetAckToReplicas();

        long targetOffset = Main.repl_offset.get();

        try {
            Thread.sleep(expireTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int acknowledged = ReplicationManager.countReplicasAcknowledged(targetOffset);

        System.out.println("Acknowledged: " + acknowledged);
        return new TextResponse(Helper.formatCount(acknowledged));
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
            System.out.println("Is it here in handleInfo?");
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
                0
        );
        return new TextResponse(Helper.formatBulkString(replicationInfo));
    }

    private RedisResponse handleReplConf(String[] args, long ack) {

        if (args.length == 3 && args[1].equals("GETACK") && args[2].equals("*")) {
            return new TextResponse(Helper.formatBulkArray("REPLCONF", "ACK", Long.toString(ack)));
        }

        System.out.println("Is it replconf?");
        return new TextResponse("+OK\r\n");
    }

    private RedisResponse handlePSync() {
        return new RDBSyncResponse("+FULLRESYNC " + Main.MASTER_REPL_ID + " " + 0 + "\r\n", Storage.dumpRDB());
    }

    private RedisResponse unknownCommand(String cmd) {
        return new TextResponse("-ERR unknown command '" + cmd.toUpperCase() + "'\r\n");
    }

    public void handleReplicaHandshakeCommand(String[] args, ConnectionContext context) throws IOException {

        String command = args[0].toUpperCase();
        switch (command) {
            case "PING" -> context.writeLine("+PONG");
            case "REPLCONF" -> {
                if (args.length == 3 && args[1].equals("ACK")) {
                    long offset = Long.parseLong(args[2]);
                    context.setAcknowledgedOffset(offset);
                    System.out.println("Replica " + context.getSocket().getRemoteSocketAddress()
                            + " acknowledged offset: " + offset);
                } else {
                    context.writeLine("+OK");
                }
            }
            case "PSYNC", "SYNC" -> new ReplicaHandshakeHandler(config).handleNewReplica(context);
        }
    }
}
