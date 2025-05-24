import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReplicationManager {

    private static final List<ConnectionContext> replicaConnections = new CopyOnWriteArrayList<>();

    public static void addReplica(ConnectionContext context) {

        replicaConnections.add(context);
        System.out.println("Added replica: " + context.getSocket().getRemoteSocketAddress());
    }

    public static void removeReplica(ConnectionContext context) {
        replicaConnections.remove(context);
        try {
            context.close();
        } catch (IOException e) {
            // ignore
        }
        System.out.println("Removed replica: " + context.getSocket().getRemoteSocketAddress());
    }

    public static void propagateToReplicas(String[] commandArgs) {

        System.out.println("Propagating to replicas: " + String.join(" ", commandArgs));
        replicaConnections.removeIf(ctx -> ctx.getSocket().isClosed());
        System.out.println("REPLICAS SIZE: " + replicaConnections.size());

        String command = Helper.formatBulkArray(commandArgs);
        byte[] bytes = command.getBytes(StandardCharsets.UTF_8);

        for (ConnectionContext connectionContext : replicaConnections) {
            try {
                OutputStream out = connectionContext.getOutput();
                out.write(bytes);
                out.flush();
            } catch (IOException e) {
                System.err.println("Error writing to replica " + connectionContext.getSocket().getRemoteSocketAddress());
                removeReplica(connectionContext);
            }
        }

        Main.repl_offset.addAndGet(bytes.length);
        System.out.println("Updating main replica offset after propagating to replicas: " + Main.repl_offset);
    }

    public static int getReplicaCount() {
        return replicaConnections.size();
    }

    public static void sendGetAckToReplicas() {

        System.out.println("Sending ack to replicas");
        byte[] getack = Helper.formatBulkArray("REPLCONF", "GETACK", "*").getBytes(StandardCharsets.UTF_8);

        for (ConnectionContext connectionContext : replicaConnections) {
            if (Main.repl_offset.get() == 0) {
                return;
            }
            CompletableFuture.runAsync(() -> {
                OutputStream output = connectionContext.getOutput();
                try {
                    output.write(getack);
                    output.flush();
                } catch (IOException e) {
                    System.err.println("Error writing to replica in sendGetAckToReplicas " + connectionContext.getSocket().getRemoteSocketAddress());
                    removeReplica(connectionContext);
                }
            });
        }
    }

    public static int countReplicasAcknowledged(long offset) {

        int count = 0;
        for (ConnectionContext connectionContext : replicaConnections) {
            if (connectionContext.getAcknowledgedOffset() >= offset) {
                count++;
            }
        }
        return count;
    }
}
