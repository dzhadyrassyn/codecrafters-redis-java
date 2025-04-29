import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    public static final String MASTER_REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final String MASTER_OFFSET = "0";

    public static void main(String[] args) throws InterruptedException {

        System.out.println("Loading configuration...");
        Config config = Config.fromArgs(args);
        System.out.println("Configuration loaded: " + config);

        Server server = new Server(config);
        Thread.startVirtualThread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                throw new RuntimeException("Server error", e);
            }
        });

        if (!config.isMaster()) {
            ReplicaClient replicaClient = new ReplicaClient(config);
            Thread.startVirtualThread(() -> {
                try {
                    replicaClient.connectToMaster();
                } catch (IOException e) {
                    throw new RuntimeException("Replica connection failed", e);
                }
            });
        }

        Thread.currentThread().join();
    }


    private static void propagateToReplicas(String[] commandArgs) throws IOException {
        System.out.println("Propagating to replicas...");
        System.out.println("Replicas amount: " + replicaSockets.size());
        replicaSockets.forEach(replicaSocket -> {
            try {
                OutputStream outputStream = replicaSocket.getOutputStream();

                outputStream.write(formatBulkArray(List.of(commandArgs)).getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    private static RedisResponse handlePSync() {

        String fullResyncResponse = formatBulkString("+FULLRESYNC " + MASTER_REPL_ID + " 0");

        String emptyRDBFileContent = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] bytes = Base64.getDecoder().decode(emptyRDBFileContent);

        return new RDBSyncResponse(fullResyncResponse, bytes);
    }

    private static RedisResponse handleReplConf() {
        return new TextResponse("+OK\r\n");
    }

    private static RedisResponse handleInfoCommand(String infoArgument, Config processConfig) {
        StringBuilder info = new StringBuilder();
        if (infoArgument.equals("replication")) {
            info.append(processConfig.isMaster() ? "role:master" : "role:slave");
        }
        info.append("\r\n").append("master_repl_offset:" + MASTER_OFFSET);

        info.append("\r\n").append("master_replid:" + MASTER_REPL_ID);

        return new TextResponse(formatBulkString(info.toString()));
    }

    private static RedisResponse handleKeyCommand(Config processConfig) {

        try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(processConfig.rdbFile()))) {
            return new TextResponse(parseRDB(bis));
        } catch (IOException e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Cannot read keys");
    }


    private static RedisResponse handleGetCommand(String key) {

        String value = storage.get(key);
        return new TextResponse((value == null) ? "$-1\r\n" : formatBulkString(value));
    }

    private static RedisResponse handleSetCommand(String[] args) {
        if (args.length < 3) throw new RuntimeException("-ERR wrong number of arguments for 'SET' command\r\n");

        String key = args[1];
        String value = args[2];
        storage.put(key, value);

        if (args.length >= 5 && args[3].equalsIgnoreCase("px")) {
            try {
                long expiryMillis = Long.parseLong(args[4]);
                scheduler.schedule(() -> storage.remove(key), expiryMillis, TimeUnit.MILLISECONDS);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unable to parse expiry milliseconds: " + args[4], e);
            }
        }


        System.out.println("Processed SET command: " + Arrays.toString(args));

        return new TextResponse("+OK\r\n");
    }

    private static RedisResponse handleConfigCommand(String[] args, Config processConfig) {
        String dirPath = processConfig.dir();
        if (args[2].equals("dir")) {
            return new TextResponse(formatBulkArray(List.of("dir", dirPath)));
        } else if (args[2].equals("dbfilename")) {
            return new TextResponse(formatBulkArray(List.of("dbfilename", dirPath + "/" + processConfig.dbfilename())));
        }
        throw new IllegalArgumentException("Unknown command: " + args[2]);
    }

}
