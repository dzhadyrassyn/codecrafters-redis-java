import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    static Map<String, String[]> map = new ConcurrentHashMap<>();

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

        try {
            OutputStream outputStream = clientSocket.getOutputStream();

            List<String> values = processInput(clientSocket.getInputStream());

            String command = values.getFirst();
            switch (command) {
                case "PING" -> outputStream.write("+PONG\r\n".getBytes());
                case "ECHO" ->
                        outputStream.write(String.format("$%d\r\n%s\r\n", values.get(1).length(), values.get(1)).getBytes());
                case "SET" -> {
                    String key = values.get(1);
                    String value = values.get(2);
                    map.put(key, new String[]{value, ""});

                    if (values.size() >= 4 && values.get(3).equals("px")) {
                        long expiryTime = System.currentTimeMillis() + Long.parseLong(values.get(4));
                        map.get(key)[1] = expiryTime + "";
                    }
                    outputStream.write("+OK\r\n".getBytes());
                }
                case "GET" -> {
                    String key = values.get(1);
                    if (map.containsKey(key)) {
                        String[] data = map.get(key);
                        if (data[1].isEmpty()) {
                            String body = data[0];
                            outputStream.write(String.format("$%d\r\n%s\r\n", body.length(), body).getBytes());
                        } else {
                            long expiryDate = Long.parseLong(data[1]);
                            if (expiryDate >= System.currentTimeMillis()) {
                                String body = data[0];
                                outputStream.write(String.format("$%d\r\n%s\r\n", body.length(), body).getBytes());
                            } else {
                                map.remove(key);
                                outputStream.write("$-1\r\n".getBytes());
                                outputStream.flush();
                            }
                        }
                    } else {
                        outputStream.write("$-1\r\n".getBytes());
                        outputStream.flush();
                    }
                }
            }

            outputStream.close();
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> processInput(InputStream inputStream) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        char[] buffer  = new char[1024];
        bufferedReader.read(buffer);

        List<String> values = new ArrayList<>();
        String[] lines = new String(buffer).split("\r\n");
        for(int i = 2; i < lines.length - 1; i += 2) {
            values.add(lines[i]);
        }

        return values;
    }
}
