import java.io.*;
import java.nio.charset.StandardCharsets;

public class ReplicaHandshakeHandler {

    private final Config config;

    public ReplicaHandshakeHandler(Config config) {
        this.config = config;
    }

    public void handleNewReplica(ConnectionContext context) throws IOException {

        OutputStream output = context.getOutput();

        String fullResync = "+FULLRESYNC " + Main.MASTER_REPL_ID + " 0\r\n";
        output.write(fullResync.getBytes(StandardCharsets.UTF_8));
        output.flush();

        File rdbFile = config.rdbFile();
        if (!rdbFile.exists()) {
            System.out.println("Warning: No RDB file found. Skipping RDB transmission.");
            output.write("$0\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        } else {
            long rdbLength = rdbFile.length();
            output.write(("$" + rdbLength + "\r\n").getBytes(StandardCharsets.UTF_8));

            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(rdbFile))) {
                bis.transferTo(output);  // Copies entire RDB to output
            }
        }

        output.flush();

        System.out.println("Replica handshake complete for " + context.getSocket().getRemoteSocketAddress());
        ReplicationManager.addReplica(context);
    }
}
