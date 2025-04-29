import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ReplicaClient {

    private final Config config;

    public ReplicaClient(Config config) {
        this.config = config;
    }

    public void connectToMaster() throws IOException {
        String host = config.masterHost();
        int port = config.masterPort();

        try (Socket socket = new Socket(host, port)) {
            System.out.println("Connected to master " + host + ":" + port);

            OutputStream output = socket.getOutputStream();
            InputStream input = socket.getInputStream();

            sendCommand(output, "ping");

            sendCommand(output, "REPLCONF", "listening-port", String.valueOf(config.replicaPort()));

            sendCommand(output, "REPLCONF", "capa", "psync2");

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
            while (true) {
                String[] args = Helper.parseRespCommand(input);
                if (args == null) {
                    System.out.println("Master closed connection");
                    break;
                }

                commandDispatcher.dispatch(args);
            }
        }
    }

    private static void sendCommand(OutputStream out, String... args) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
