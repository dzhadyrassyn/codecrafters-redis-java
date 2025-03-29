import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    static Map<String, String> storage = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

        int port = 6379;

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        try(ServerSocket serverSocket = new ServerSocket(port)) {

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

    private static void handleClientRequest(Socket clientSocket) {

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream outputStream = clientSocket.getOutputStream();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = processInput(line, reader);

                String command = values[0];
                switch (command) {
                    case "PING" -> outputStream.write("+PONG\r\n".getBytes());
                    case "ECHO" ->
                            outputStream.write(String.format("$%d\r\n%s\r\n", values[1].length(), values[1]).getBytes());
                    case "SET" -> {
                        String key = values[1];
                        String value = values[2];
                        storage.put(key, value);

                        if (values.length >= 4 && values[3].equals("px")) {
                            long expiryTime = Long.parseLong(values[4]);
                            scheduledExecutorService.schedule(() -> storage.remove(key), expiryTime, TimeUnit.MILLISECONDS);
                        }
                        outputStream.write("+OK\r\n".getBytes());
                    }
                    case "GET" -> {
                        String key = values[1];
                        String value = storage.getOrDefault(key, null);
                        if (value == null) {
                            outputStream.write("$-1\r\n".getBytes());
                        } else {
                            outputStream.write(String.format("$%d\r\n%s\r\n", value.length(), value).getBytes());
                        }
                    }
                }
            }

            outputStream.flush();
            outputStream.close();
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] processInput(String line, BufferedReader reader) throws IOException {

        if (line == null || !line.startsWith("*")) {
            throw new IOException("Wrong redis input format");  // Empty if no valid command
        }

        // Read array length
        int numElements = Integer.parseInt(line.substring(1));

        String[] result = new String[numElements];
        for (int i = 0; i < numElements; i++) {
            String lengthLine = reader.readLine(); // Read "$<length>"
            int length = Integer.parseInt(lengthLine.substring(1)); // Extract length
            char[] buffer = new char[length];
            reader.read(buffer, 0, length);  // Read exact number of characters
            reader.readLine();  // Consume the trailing \r\n

            result[i] = new String(buffer);
        }

        return result;
    }
}
