import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RequestHandler {

    public static void handle(Socket socket, Config config) throws IOException {
        try (
                InputStream input = socket.getInputStream();
                OutputStream output = socket.getOutputStream()
        ) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("*")) {
                    int argCount = Integer.parseInt(line.substring(1));
                    String[] args = new String[argCount];

                    for (int i = 0; i < argCount; i++) {
                        String lenLine = reader.readLine(); // $<length>
                        int len = Integer.parseInt(lenLine.substring(1));
                        char[] buffer = new char[len];
                        reader.read(buffer);
                        reader.readLine(); // skip trailing \r\n
                        args[i] = new String(buffer);
                    }

                    String response = CommandDispatcher.dispatch(args, config);
                    if (response != null) {
                        output.write(response.getBytes(StandardCharsets.UTF_8));
                        output.flush();
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Client disconnected or error occurred: " + e.getMessage());
        }
    }
}
