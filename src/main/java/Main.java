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
            OutputStream outputStream = clientSocket.getOutputStream();
            String[] values = processInput(clientSocket.getInputStream());

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
                        long expiryTime = System.currentTimeMillis() + Long.parseLong(values[4]);
                        scheduledExecutorService.schedule(() -> storage.remove(key), expiryTime, TimeUnit.MICROSECONDS);
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

            outputStream.close();
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] processInput(InputStream inputStream) throws IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String firstLine = reader.readLine();
        if (firstLine.charAt(0) != '*') throw new IOException("Wrong redis input format");

        // Read array length
        int numElements = Integer.parseInt(firstLine.substring(1));

        String[] result = new String[numElements];
        for (int i = 0; i < numElements; i++) {
            reader.readLine();  // Read "$3"
            result[i] = reader.readLine();  // Read actual string
        }

        return result;
    }
}
