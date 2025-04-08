import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

    private static File getRDBFile() {
        String fileName = programArgs.get("dir") + "/" + programArgs.get("dbfilename");
        File file = new File(fileName);
        if (!file.exists()) {
            throw new RuntimeException("File not found: " + fileName);
        }

        return file;
    }

    private static String handleKeyCommand(String arg) {

        try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(getRDBFile()))) {
            return parseRDB(bis);
        } catch (IOException e) {
            e.printStackTrace();
        }

        throw new RuntimeException("Cannot read keys");
    }

    private static String parseRDB(BufferedInputStream bis) throws IOException {
        DataInputStream dis = new DataInputStream(bis);

        String magicString = readMagicString(dis);
        if (!magicString.equals("REDIS")) {
            throw new IOException("Not a valid RDB file");
        }

        String versionNumber = readVersion(dis);
        System.out.println("HEADER SECTION: " + magicString + versionNumber);

        List<String> values = new ArrayList<>();

        while (dis.available() > 0) {
            int opCode = dis.readUnsignedByte();

            if (opCode == 0xFA) { // Metadata section
                String key = readString(dis);
                String value = readString(dis);
                System.out.println("Metadata: " + key + " = " + value);
            } else if (opCode == 0xFE) { // Database selector
                int dbNumber = dis.readUnsignedByte();
                System.out.println("\nSwitched to database: " + dbNumber);
            } else if (opCode == 0xFB) { // RDB_OPCODE_RESIZEDB
                int dbHashTableSize = readLength(dis);
                int expiryHashTableSize = readLength(dis);
                System.out.printf("Resize DB - keys: %d, expires: %d%n", dbHashTableSize, expiryHashTableSize);
            } else if (opCode == 0xFD) { // Expiry time (seconds)
                int expiry = dis.readInt();
                System.out.println("Key with expiry: " + expiry);
            } else if (opCode == 0xFC) { // Expiry time (milliseconds)
                long expiry = dis.readLong();
                System.out.println("Key with expiry (ms): " + expiry);
            } else if (opCode == 0xFF) { // End of RDB file
                System.out.println("End of RDB file.");
                break;
            } else {
                // Read Key
                String key = readString(dis);
                String value = readString(dis);
                System.out.println("Key: " + key + ", Value: " + value);
                values.add(key);
//                values.add(value);
            }
        }

        return formatBulkArray(values);
    }

    private static int readLength(DataInputStream dis) throws IOException {
        int first = dis.readUnsignedByte();
        if ((first & 0xC0) == 0x00) {
            return first & 0x3F;
        } else if ((first & 0xC0) == 0x40) {
            int second = dis.readUnsignedByte();
            return ((first & 0x3F) << 8) | second;
        } else if ((first & 0xC0) == 0x80) {
            return dis.readInt();
        } else {
            throw new IOException("Unsupported length encoding: " + String.format("0x%02X", first));
        }
    }

    private static String readMagicString(DataInputStream dis) throws IOException {

        byte[] magic = new byte[5];
        dis.readFully(magic);
        return new String(magic, StandardCharsets.UTF_8);
    }

    private static String readVersion(DataInputStream dis) throws IOException {
        byte[] versionBytes = new byte[4];
        dis.readFully(versionBytes);
        return new String(versionBytes, StandardCharsets.UTF_8);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int firstByte = dis.readUnsignedByte();

        if ((firstByte & 0xC0) == 0x00) {
            // 6-bit length string
            int length = firstByte & 0x3F;
            byte[] bytes = dis.readNBytes(length);
            return new String(bytes);
        } else if ((firstByte & 0xC0) == 0x40) {
            // 14-bit length string
            int secondByte = dis.readUnsignedByte();
            int length = ((firstByte & 0x3F) << 8) | secondByte;
            byte[] bytes = dis.readNBytes(length);
            return new String(bytes);
        } else if (firstByte == 0x80) {
            // 32-bit length string
            int length = dis.readInt();
            byte[] bytes = dis.readNBytes(length);
            return new String(bytes);
        } else if (firstByte == 0xC0) {
            // Encoded as 8-bit int (INT8)
            int value = dis.readByte();
            return Integer.toString(value);
        } else if (firstByte == 0xC1) {
            // Encoded as 16-bit int (INT16), little-endian
            short value = Short.reverseBytes(dis.readShort());
            return Short.toString(value);
        } else if (firstByte == 0xC2) {
            // Encoded as 32-bit int (INT32), little-endian
            int value = Integer.reverseBytes(dis.readInt());
            return Integer.toString(value);
        } else {
            throw new IOException("Unsupported string encoding, first byte: " + String.format("0x%02X", firstByte));
        }
    }

    private static String formatBulkString(String value) {
        return String.format("$%d\r\n%s\r\n", value.length(), value);
    }

    private static String formatBulkArray(List<String> args) {

        StringBuilder response = new StringBuilder();
        response.append("*").append(args.size()).append("\r\n");
        for(String arg : args) {
            response.append(formatBulkString(arg));
        }
        return response.toString();
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
            return formatBulkArray(List.of("dir", dirPath));
        } else if (args[2].equals("dbfilename")) {
            return formatBulkArray(List.of("dbfilename", dirPath + "/" + programArgs.get("dbfilename")));
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
