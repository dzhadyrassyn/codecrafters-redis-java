import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

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
            char[] buffer = new char[length];
            InputStreamReader reader = new InputStreamReader(input, StandardCharsets.UTF_8);
            reader.read(buffer, 0, length);

            readLine(input); // consume trailing \r\n
            args[i] = new String(buffer);
        }

        return args;
    }
}
