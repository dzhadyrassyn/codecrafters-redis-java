import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Main {

    private static final int DEFAULT_PORT = 6379;
    private static final int THREAD_POOL_SIZE = 10;
    private static final Map<String, String> storage = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
    private static final Map<String, String> programArgs = new ConcurrentHashMap<>();
    private static boolean IS_MASTER = true;

    public static void main(String[] args) {

        System.out.println("Processing input args...");
        processInputArgs(args);

        int redisPort = getPort();
        setIsMasterInstance();

        if (!IS_MASTER) {
            sendHandshake();
        }
        File rdbFile = getRDBFile();
        if (rdbFile.exists()) {
            System.out.println("Parsing RDB file...");
            try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(getRDBFile()))) {
                parseRDB(bis);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Cannot read keys");
            }
        }

        System.out.printf("Redis-like %s server is starting on port %d ...%n", IS_MASTER ? "MASTER" : "REPLICA", redisPort);

        try(ServerSocket serverSocket = new ServerSocket(redisPort);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE)
        ) {

            serverSocket.setReuseAddress(true);

            while(true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("accepted new connection");
                executorService.submit(() -> handleClientRequest(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void sendHandshake() {
        String[] masterInfo = programArgs.get("replicaof").split(" ");
        String host = masterInfo[0];
        int port = Integer.parseInt(masterInfo[1]);

        try(Socket socket = new Socket(host, port)) {
            String ping = "*1\r\n$4\r\nping\r\n";
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(ping.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String masterResponse = bufferedReader.readLine();
            System.out.println("Response from master to PING: " + masterResponse);

            if (masterResponse.equalsIgnoreCase("+PONG")) {
                outputStream.write(formatBulkArray(List.of("REPLCONF", "listening-port", Integer.toString(getPort()))).getBytes(StandardCharsets.UTF_8));
                outputStream.flush();

                masterResponse = bufferedReader.readLine();
                System.out.println("Response from master to REPLCONF listening-port " + getPort() + ": " + masterResponse);

                if (masterResponse.equalsIgnoreCase("+OK")) {
                    outputStream.write(formatBulkArray(List.of("REPLCONF", "capa", "psync2")).getBytes(StandardCharsets.UTF_8));
                    outputStream.flush();

                    masterResponse = bufferedReader.readLine();
                    System.out.println("Response from master to REPLCONF capa psync2: " + masterResponse);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setIsMasterInstance() {
        IS_MASTER = !programArgs.containsKey("replicaof");
    }

    private static int getPort() {
        if (programArgs.containsKey("port")) {
            return Integer.parseInt(programArgs.get("port"));
        }
        return DEFAULT_PORT;
    }

    private static void processInputArgs(String[] args) {
        for(int i = 0; i < args.length; i++) {
            if(args[i].startsWith("--")) {
                programArgs.put(args[i].substring(2), args[i + 1]);
                ++i;
            }
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
            case "KEYS" -> handleKeyCommand();
            case "INFO" -> handleInfoCommand(args[1]);
            case "REPLCONF" -> handleReplConf();
            default -> "-ERR Unknown command\r\n";
        };
    }

    private static String handleReplConf() {
        return "+OK\r\n";
    }

    private static String handleInfoCommand(String infoArgument) {
        StringBuilder info = new StringBuilder();
        if (infoArgument.equals("replication")) {
            info.append(IS_MASTER ? "role:master" : "role:slave");
        }
        info.append("\r\n").append("master_repl_offset:0");
        info.append("\r\n").append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");

        return formatBulkString(info.toString());
    }

    private static File getRDBFile() {

        String fileName = programArgs.get("dir") + "/" + programArgs.get("dbfilename");
        return new File(fileName);
    }

    private static String handleKeyCommand() {

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
                int expiry = readLittleEndianInt(dis);
                System.out.println("Key with expiry: " + expiry);
                int valueType = dis.readUnsignedByte();  // ⬅️ MUST read this byte
                if (valueType == 0x00) { // string
                    saveKeyValueToStorage(dis, TimeUnit.SECONDS.toMillis(expiry));
                } else {
                    throw new IOException("Unsupported value type after expiry (0xFD): " + valueType);
                }
            } else if (opCode == 0xFC) { // Expiry time (milliseconds)
                long expiry = readLittleEndianLong(dis);
                System.out.println("Key with expiry (ms): " + expiry);
                int valueType = dis.readUnsignedByte();  // ⬅️ MUST read this byte
                if (valueType == 0x00) {
                    saveKeyValueToStorage(dis, expiry);
                } else {
                    throw new IOException("Unsupported value type after expiry (0xFC): " + valueType);
                }
            } else if (opCode == 0xFF) { // End of RDB file
                System.out.println("End of RDB file.");
                break;
            } else {
                // Read Key
                saveKeyValueToStorage(dis, 0L);
            }
        }

        return formatBulkArray(storage.keySet().stream().toList());
    }

    private static int readLittleEndianInt(DataInputStream dis) throws IOException {
        byte[] bytes = new byte[4];
        dis.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static long readLittleEndianLong(DataInputStream dis) throws IOException {
        byte[] bytes = new byte[8];
        dis.readFully(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static void saveKeyValueToStorage(DataInputStream dis, long expiry) throws IOException {
        String key = readString(dis);
        String value = readString(dis);
        System.out.println("Key: " + key + ", Value: " + value);

        if (expiry != 0L) {
            handleSetCommand(new String[]{"SET", key, value, "px", Long.toString(expiry - System.currentTimeMillis())});
        } else {
            handleSetCommand(new String[]{"SET", key, value});
        }
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
