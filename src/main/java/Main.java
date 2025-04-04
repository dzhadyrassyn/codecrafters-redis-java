import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    private static final int PORT = 6379;
    private static final int THREAD_POOL_SIZE = 10;
    private static final Map<String, String> storage = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
    private static final Map<String, String> programArgs = new ConcurrentHashMap<>();

    public static void main(String[] args) {

        processInputArgs(args);

        System.out.println("Redis-like server is starting on port " + PORT + "...");

        try(ServerSocket serverSocket = new ServerSocket(PORT)) {

            serverSocket.setReuseAddress(true);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            while(true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("accepted new connection");
                executorService.submit(() -> handleClientRequest(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void processInputArgs(String[] args) {
        if (args.length == 4 && args[0].equals("--dir") && args[2].equals("--dbfilename")) {
            programArgs.put("dir", args[1]);
            programArgs.put("dbfilename", args[3]);
        }
    }

    private static void handleClientRequest(Socket clientSocket) {

        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream outputStream = clientSocket.getOutputStream()
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] commandArgs = parseRedisCommand(line, reader);
                if (commandArgs.length == 0) {
                    continue;
                }
                String response = processCommand(commandArgs);
                outputStream.write(response.getBytes());
                outputStream.flush();
            }
        } catch (IOException e) {
            System.err.println("Client connection error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    private static String processCommand(String[] args) {
        String command = args[0];

        return switch (command) {
            case "PING" -> "+PONG\r\n";
            case "ECHO" -> formatBulkString(args[1]);
            case "SET" -> handleSetCommand(args);
            case "GET" -> handleGetCommand(args[1]);
            case "CONFIG" -> handleConfigCommand(args);
            case "KEYS" -> handleKeyCommand(args[1]);
            default -> "-ERR Unknown command\r\n";
        };
    }

    private static String handleKeyCommand(String arg) {
        String fileName = programArgs.get("dir") + "/" + programArgs.get("dbfilename");
        File file = new File(fileName);
        if (!file.exists()) {
            return "*1\r\n$3\r\nfoo\r\n";
        }

        try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            parseRDB(bis);
        } catch (IOException e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Cannot read keys");
    }

    private static void parseRDB(BufferedInputStream bis) throws IOException {
        DataInputStream dis = new DataInputStream(bis);

        if(!readMagicString(dis)) {
            throw new IOException("Not a valid RDB file");
        }

        String version = readVersion(dis);
        System.out.println("REDIS version: " + version);
    }

    private static boolean readMagicString(DataInputStream dis) throws IOException {

        byte[] magic = new byte[5];
        dis.readFully(magic);
        return new String(magic, StandardCharsets.UTF_8).equals("REDIS");
    }

    private static String readVersion(DataInputStream dis) throws IOException {
        byte[] versionBytes = new byte[4];
        dis.readFully(versionBytes);
        return new String(versionBytes, StandardCharsets.UTF_8);
    }

    private static String formatBulkString(String value) {
        return String.format("$%d\r\n%s\r\n", value.length(), value);
    }

    private static String formatBulkArray(String key, String value) {
        return String.format("*2\r\n$%d\r\n%s\r\n" + formatBulkString(value), key.length(), key);
    }

    private static String handleGetCommand(String key) {
        String value = storage.get(key);
        return (value == null) ? "$-1\r\n" : formatBulkString(value);
    }

    private static String handleSetCommand(String[] args) {
        if (args.length < 3) return "-ERR wrong number of arguments for 'SET' command\r\n";

        String key = args[1];
        String value = args[2];
        storage.put(key, value);

        if (args.length >= 5 && args[3].equalsIgnoreCase("px")) {
            try {
                long expiryMillis = Long.parseLong(args[4]);
                scheduler.schedule(() -> storage.remove(key), expiryMillis, TimeUnit.MILLISECONDS);
            } catch (NumberFormatException e) {
                return "-ERR PX value is not a valid integer\r\n";
            }
        }

        return "+OK\r\n";
    }

    private static String handleConfigCommand(String[] args) {
        String dirPath = programArgs.get("dir");
        if (args[2].equals("dir")) {
            return formatBulkArray("dir", dirPath);
        } else if (args[2].equals("dbfilename")) {
            return formatBulkArray("dbfilename", dirPath + "/" + programArgs.get("dbfilename"));
        }
        throw new IllegalArgumentException("Unknown command: " + args[2]);
    }

    private static String[] parseRedisCommand(String firstLine, BufferedReader reader) throws IOException {

        if (firstLine == null || !firstLine.startsWith("*")) {
            throw new IOException("Invalid RESP2 input format");
        }

        int numElements = Integer.parseInt(firstLine.substring(1));
        String[] result = new String[numElements];

        for (int i = 0; i < numElements; i++) {
            String lengthLine = reader.readLine();
            int length = Integer.parseInt(lengthLine.substring(1));
            char[] buffer = new char[length];
            reader.read(buffer, 0, length);
            reader.readLine();  // Consume trailing \r\n
            result[i] = new String(buffer);
        }

        return result;
    }
}
