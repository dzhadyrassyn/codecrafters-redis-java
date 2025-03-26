import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

        int port = 6379;
        try(ServerSocket serverSocket = new ServerSocket(port)) {

            serverSocket.setReuseAddress(true);

            Socket clientSocket = serverSocket.accept();
            System.out.println("accepted new connection");

            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String line;
            while ((line = bufferedReader.readLine()) != null && !line.isEmpty()) {
                if (line.equals("PING")) {
                    outputStream.write("+PONG\r\n".getBytes());
                }
            }

            outputStream.close();
            clientSocket.close();

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
