import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ReplicaClient {

    private final Config config;

    public ReplicaClient(Config config) {
        this.config = config;
    }

    public void connectToMaster() throws IOException {

        String host = config.masterHost();
        int port = config.masterPort();

        Socket socket = new Socket(host, port);
        ConnectionContext context = new ConnectionContext(socket);
        System.out.println("Connected to master " + host + ":" + port);

        OutputStream output = context.getOutput();
        InputStream input = context.getInput();

        sendCommand(output, "ping");
        System.out.println("Response from master is " + Helper.readLine(input));

        sendCommand(output, "REPLCONF", "listening-port", String.valueOf(config.replicaPort()));
        System.out.println("Response from master is " + Helper.readLine(input));

        sendCommand(output, "REPLCONF", "capa", "psync2");
        System.out.println("Response from master is " + Helper.readLine(input));

        sendCommand(output, "PSYNC", "?", "-1");

        String fullResyncLine = Helper.readLine(input);
        if (!fullResyncLine.startsWith("+FULLRESYNC")) {
            throw new IOException("Expected FULLRESYNC, got: " + fullResyncLine);
        }
        System.out.println("Full resync: " + fullResyncLine);

        String lengthLine = Helper.readLine(input);
        if (!lengthLine.startsWith("$")) {
            throw new IOException("Expected bulk length for RDB file, got: " + lengthLine);
        }
        int rdbLength = Integer.parseInt(lengthLine.substring(1));

        byte[] rdbBytes = input.readNBytes(rdbLength);
        BufferedInputStream rdbStream = new BufferedInputStream(new ByteArrayInputStream(rdbBytes));
        RDBParser.parseRDB(rdbStream);
        System.out.println("Finished initial RDB sync");

        CommandDispatcher commandDispatcher = new CommandDispatcher(config);

        long ackOffset = 0;
        while (true) {
            CountingInputStream counter = new CountingInputStream(input);
            String[] args = Helper.parseRespCommand(counter);

            System.out.println("Replica received: " + Arrays.toString(args));
            if (args == null) {
                System.out.println("Master closed connection");
                break;
            }

            commandDispatcher.dispatch(args);
            String commandFromMaster = String.join(" ", args);
            if (commandFromMaster.equals("REPLCONF GETACK *")) {
                context.write(Helper.formatBulkArray("REPLCONF", "ACK", Long.toString(ackOffset)));
            }
            ackOffset += counter.getCount();
        }

    }

    private static void sendCommand(OutputStream out, String... args) throws IOException {

        System.out.println("Sending command to master: " + Arrays.toString(args));
        String sendCommand = Helper.formatBulkArray(args);
        out.write(sendCommand.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
