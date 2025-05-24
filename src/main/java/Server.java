import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Server {

    private final Config config;

    public Server(Config config) {
        this.config = config;
    }

    private static final List<String> replicaHandshakeCommands = List.of("PING", "REPLCONF", "PSYNC", "SYNC");

    public void start() throws IOException {

        int port = config.getPort();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server started on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New connection from " + socket.getRemoteSocketAddress());

                Thread.startVirtualThread(() -> {
                    try {
                        handleClientRequest(socket);
                    } catch (IOException ex) {
                        System.err.println("Error handling request " + ex.getMessage());
                        ex.printStackTrace();
                    }
                });
            }
        }
    }

    private void handleClientRequest(Socket socket) throws IOException {

        CommandDispatcher commandDispatcher = new CommandDispatcher(config);

        try(ConnectionContext context = new ConnectionContext(socket)) {

            BufferedInputStream input = context.getInput();

            while (true) {
                String[] args = Helper.parseRespCommand(input);
                if (args == null) {
                    System.out.println("Client disconnected");
                    break;
                }

                System.out.println("Received request: " + String.join(" ", args));

                String firstCommand = args[0].toUpperCase();
                if (replicaHandshakeCommands.contains(firstCommand)) {
                    commandDispatcher.handleReplicaHandshakeCommand(args, context);
                } else {
                    new RequestHandler(config).handleWithPreloadedCommand(context, args);
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client in handleClientRequest: " + e.getMessage());
        }
    }
}
