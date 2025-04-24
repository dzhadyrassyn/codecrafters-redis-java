import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

    private final Config config;

    public Server(Config config) {
        this.config = config;
    }

    public void start() throws IOException {

        int port = config.getPort();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New connection from " + socket.getRemoteSocketAddress());

                Thread.startVirtualThread(() -> {
                    try (socket) {
                        Main.handleClientRequest(socket, config);
                    } catch (IOException ex) {
                        System.err.println("Error handling request " + ex.getMessage());
                        ex.printStackTrace();
                    }
                });
            }
        }
    }
}
