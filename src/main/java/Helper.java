import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Helper {

    public static String readLine(InputStream input) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = input.read()) != -1) {
            if (b == '\r') {
                int next = input.read();
                if (next == '\n') break;
                buffer.write(b);
                buffer.write(next);
            } else {
                buffer.write(b);
            }
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    public static String[] parseRespCommand(InputStream input) throws IOException {
        String firstLine = readLine(input);

        if (firstLine == null || firstLine.isEmpty()) {
            return null; // Client disconnected
        }

        if (!firstLine.startsWith("*")) {
            throw new IOException("Invalid RESP input: " + firstLine);
        }

        int argCount = Integer.parseInt(firstLine.substring(1));
        String[] args = new String[argCount];

        for (int i = 0; i < argCount; i++) {
            String lengthLine = readLine(input); // $<length>
            if (lengthLine == null || !lengthLine.startsWith("$")) {
                throw new IOException("Invalid bulk string length line: " + lengthLine);
            }

            int length = Integer.parseInt(lengthLine.substring(1));
            byte[] buffer = input.readNBytes(length);

            readLine(input); // consume trailing \r\n
            args[i] = new String(buffer);
        }

        return args;
    }

    public static String formatBulkString(String value) {

        return String.format("$%d\r\n%s\r\n", value.length(), value);
    }

    public static String formatBulkArray(List<String> args) {

        StringBuilder response = new StringBuilder();
        response.append("*").append(args.size()).append("\r\n");
        for(String arg : args) {
            response.append(formatBulkString(arg));
        }
        return response.toString();
    }
}
