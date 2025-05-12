import java.io.*;
import java.nio.charset.StandardCharsets;

public class ReplicaHandshakeHandler {

    private final Config config;

    public ReplicaHandshakeHandler(Config config) {
        this.config = config;
    }

    public void handleNewReplica(ConnectionContext context) throws IOException {
        System.out.println("handleNewReplica is called");
        OutputStream output = context.getOutput();

        output.write(("+FULLRESYNC " + Main.MASTER_REPL_ID + " " + 0 + "\r\n").getBytes(StandardCharsets.UTF_8));
        byte[] rdbBytes = Storage.dumpRDB();
        output.write(("$" + rdbBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        output.write(rdbBytes);
        output.flush();

        System.out.println("Replica handshake complete for " + context.getSocket().getRemoteSocketAddress());
        ReplicationManager.addReplica(context);
    }

}
