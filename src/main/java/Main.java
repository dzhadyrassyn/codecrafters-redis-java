import java.io.IOException;

public class Main {

    public static final String MASTER_REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    public static final String MASTER_OFFSET = "0";

    public static void main(String[] args) throws InterruptedException {

        System.out.println("Loading configuration...");
        Config config = Config.fromArgs(args);
        System.out.println("Configuration loaded: " + config);

        Server server = new Server(config);
        Thread.startVirtualThread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                throw new RuntimeException("Server error", e);
            }
        });

        if (!config.isMaster()) {
            ReplicaClient replicaClient = new ReplicaClient(config);
            Thread.startVirtualThread(() -> {
                try {
                    replicaClient.connectToMaster();
                } catch (IOException e) {
                    throw new RuntimeException("Replica connection failed", e);
                }
            });
        }

        Thread.currentThread().join();
    }
}
