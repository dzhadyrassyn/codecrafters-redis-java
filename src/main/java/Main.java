import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    static Map<String, String> map = new ConcurrentHashMap<>();

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
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("PING")) {
                    outputStream.write("+PONG\r\n".getBytes());
                } else if (line.startsWith("ECHO")) {
                    bufferedReader.readLine();
                    String body = bufferedReader.readLine();
                    outputStream.write(String.format("$%d\r\n%s\r\n", body.length(), body).getBytes());
                } else if (line.startsWith("SET")) {
                    bufferedReader.readLine();
                    String key = bufferedReader.readLine();
                    bufferedReader.readLine();
                    String value = bufferedReader.readLine();
                    map.put(key, value);
                    outputStream.write("+OK\r\n".getBytes());
                } else if (line.startsWith("GET")) {
                    bufferedReader.readLine();
                    String key = bufferedReader.readLine();
                    if (map.containsKey(key)) {
                        outputStream.write(String.format("$%d\r\n%s\r\n", map.get(key).length(), map.get(key)).getBytes());
                    } else {
                        outputStream.write("$-1\r\n".getBytes());
                    }
                }
            }

            outputStream.close();
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
