import java.io.IOException;
import java.util.*;

public class CommandDispatcher {

    private final Config config;

    public CommandDispatcher(Config config) {
        this.config = config;
    }

    public RedisResponse dispatch(String[] args, long ack, ConnectionContext ctx) {

//        System.out.println("Processing command: " + Arrays.toString(args));

        if (args == null || args.length == 0) {
            return new ErrorResponse("ERR Empty command");
        }

        String command = args[0].toUpperCase();

        if (ctx.isInTransaction() && !command.equals("EXEC")) {
            ctx.queueTransactionCommand(args);
            return new SimpleStringResponse("QUEUED");
        }

        return dispatchInternal(args, ack, ctx);
    }

    private RedisResponse dispatchInternal(String[] args, long ack, ConnectionContext ctx) {

        String command = args[0].toUpperCase();

        return switch (command) {
            case "PING" -> new SimpleStringResponse("PONG");
            case "ECHO" -> new TextResponse(args[1]);
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
            case "XREAD" -> handleXRead(args);
            case "INCR" -> handleIncrCommand(args);
            case "MULTI" -> handleMultiCommand(args, ctx);
            case "EXEC" -> handleExecCommand(args, ctx);
            case "DISCARD" -> handleTransactionDiscardCommand(args, ctx);
            default -> unknownCommand(command);
        };
    }

    private RedisResponse handleTransactionDiscardCommand(String[] args, ConnectionContext ctx) {

        ctx.finishTransaction();
        ctx.clearTransactionCommands();

        return new SimpleStringResponse("OK");
    }

    private RedisResponse handleExecCommand(String[] args, ConnectionContext ctx) {

        if (!ctx.isInTransaction()) {
            return new ErrorResponse("ERR EXEC without MULTI");
        }

        List<SimpleResponse> transactionCommandResults = new ArrayList<>();
        while (!ctx.isTransactionCommandsEmpty()) {
            String[] transactionCommand = ctx.dequeueTransactionCommand();
            SimpleResponse dispatch = (SimpleResponse) dispatchInternal(transactionCommand, 0, ctx);
            transactionCommandResults.add(dispatch);
        }
        ctx.finishTransaction();
        return new BulkArrayResponse(transactionCommandResults);
    }

    private RedisResponse handleMultiCommand(String[] args, ConnectionContext ctx) {

        ctx.startTransaction();
        return new SimpleStringResponse("OK");
    }

    private RedisResponse handleIncrCommand(String[] args) {

        String key = args[1];
        String value = Storage.get(key);
        if (value != null) {
            try {
                int newValue = Integer.parseInt(value) + 1;
                Storage.set(key, String.valueOf(newValue));
                return new IntegerResponse(newValue);
            } catch (NumberFormatException e) {
                return new ErrorResponse("ERR value is not an integer or out of range");
            }
        }
        int startCount = 1;
        Storage.set(key, String.valueOf(startCount));

        return new IntegerResponse(startCount);
    }

    private RedisResponse handleXRead(String[] args) {

        int k = 2;
        if (args[1].equals("block")) {
            k = 4;
        }
        List<String> streams = new ArrayList<>();
        List<String> times = new ArrayList<>();
        for(int i = k; i < args.length; i++) {
            String currentArg = args[i];
            if (currentArg.charAt(0) >= '0' && currentArg.charAt(0) <= '9') {
                times.add(currentArg);
            } else if (currentArg.charAt(0) == '$') {
                Optional<StreamEntry> last = StreamStorage.getLast(streams.getLast());
                String time = last.map(streamEntry -> streamEntry.id().toString()).orElse("-");
                times.add(time);
            } else {
                streams.add(currentArg);
            }
        }

        if (!args[1].equals("block")) {
            return new XReadResponse(fetchXRead(streams, times), streams);
        }

        long blockTime = Long.parseLong(args[2]);
        long deadline = System.currentTimeMillis() + blockTime;
        Object lock = StreamLocks.getLock(streams.getFirst());
        synchronized (lock) {
            while (true) {
                long leftTime = deadline - System.currentTimeMillis();
                if (leftTime <= 0 && blockTime != 0) break;

                try {
                    lock.wait(blockTime == 0 ? 0 : leftTime);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted");
                    Thread.currentThread().interrupt();
                    return new NullStringResponse();
                }

                Map<String, List<StreamEntry>> entries = fetchXRead(streams, times);
                boolean hasEmptyData = entries.values().stream().anyMatch(List::isEmpty);
                if (!hasEmptyData) {
                    return new XReadResponse(entries, streams);
                }
                if (blockTime == 0) {
                    break;
                }
            }
        }

        return new NullStringResponse();
    }

    private Map<String, List<StreamEntry>> fetchXRead(List<String> streams, List<String> times) {

        Map<String, List<StreamEntry>> entries = new HashMap<>();

        for (int i = 0; i < streams.size(); i++) {
            List<StreamEntry> data = StreamStorage.read(streams.get(i), times.get(i));
            entries.put(streams.get(i), data);
        }

        return entries;
    }

    private RedisResponse handleXRange(String[] args) {

        String streamName = args[1];
        String from = args[2];
        String to = args[3];

        List<StreamEntry> data = StreamStorage.fetch(streamName, from, to);

        return new XRangeResponse(data);
    }

    private RedisResponse handleXAdd(String[] args) {

        String streamName = args[1];
        String[] streamArgs = Arrays.copyOfRange(args, 3, args.length);

        String id = args[2];
        try {
            id = StreamStorage.add(streamName, id, streamArgs);
        } catch (InvalidStreamIdArgumentException e) {
            return new ErrorResponse(e.getMessage());
        }
        return new TextResponse(id);
    }

    private RedisResponse handleType(String[] args) {

        String key = args[1];
        String value = Storage.get(key);
        if (value != null) {
            return new SimpleStringResponse("string");
        }

        boolean isStream = StreamStorage.contains(key);
        if (isStream) {
            return new SimpleStringResponse("stream");
        }

        return new SimpleStringResponse("none");
    }

    private RedisResponse handleWait(String[] args) {

        long expectedReplicas = Long.parseLong(args[1]);
        if (expectedReplicas == 0) {
            return new IntegerResponse(0);
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
        return new IntegerResponse(acknowledged);
    }

    private RedisResponse handleSetCommand(String[] args) {

//        System.out.println("Processing SET command");
        String key = args[1];
        String value = args[2];
        Storage.set(key, value);

        if (args.length >= 5 && args[3].equalsIgnoreCase("px")) {
            long ttlMillis = Long.parseLong(args[4]);
            Storage.setWithExpiry(key, value, ttlMillis);
        }

        ReplicationManager.propagateToReplicas(args);
        if (config.isMaster()) {
            return new SimpleStringResponse("OK");
        }

        return null;
    }

    private RedisResponse handleGetCommand(String[] args) {

        String value = Storage.get(args[1]);
        if (value == null) {
            return new NullStringResponse();
        }
        return new TextResponse(value);
    }

    private RedisResponse handleConfigCommand(String[] args) {

        if (args.length >= 3 && args[1].equalsIgnoreCase("GET")) {
            String configKey = args[2];
            if (configKey.equals("dir")) {
                return new BulkStringArrayResponse(List.of(configKey, config.dir()));
            } else if (configKey.equals("dbfilename")) {
                return new BulkStringArrayResponse(List.of(configKey, config.dbfilename()));
            }
        }
        return new BulkStringArrayResponse(List.of());
    }

    private RedisResponse handleKeyCommand() {
        return new BulkStringArrayResponse(Storage.keys());
    }

    private RedisResponse handleInfoCommand(String[] args) {

        if (args.length < 2 || !args[1].equalsIgnoreCase("replication")) {
            System.out.println("Is it here in handleInfo?");
            return new SimpleStringResponse("OK");
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
        return new TextResponse(replicationInfo);
    }

    private RedisResponse handleReplConf(String[] args, long ack) {

        if (args.length == 3 && args[1].equals("GETACK") && args[2].equals("*")) {
            return new BulkStringArrayResponse(List.of("REPLCONF", "ACK", Long.toString(ack)));
        }

        return new SimpleStringResponse("OK");
    }

    private RedisResponse handlePSync() {
        return new RDBSyncResponse("+FULLRESYNC " + Main.MASTER_REPL_ID + " " + 0 + "\r\n", Storage.dumpRDB());
    }

    private RedisResponse unknownCommand(String cmd) {
        return new ErrorResponse("ERR unknown command '" + cmd.toUpperCase());
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
