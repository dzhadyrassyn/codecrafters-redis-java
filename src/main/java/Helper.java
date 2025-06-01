import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static String formatSimpleError(String value) {

        return String.format("-%s\r\n", value);
    }

    public static String formatBulkArray(String... args) {

        StringBuilder response = new StringBuilder();
        response.append("*").append(args.length).append("\r\n");
        for(String arg : args) {
            response.append(formatBulkString(arg));
        }
        return response.toString();
    }

    public static String formatType(String value) {

        return String.format("+%s\r\n", value);
    }

    public static String formatCount(int count) {

        return String.format(":%d\r\n", count);
    }

    public static Map<String, String> parseFieldValuePairs(String... values) {

        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i], values[i + 1]);
        }
        return map;
    }

    public static String formatXRange(List<StreamEntry> data) {

        int size = data.size();
        StringBuilder response = new StringBuilder();
        response.append("*").append(data.size()).append("\r\n");
        for (int i = 0; i < size; i++) {
            StreamEntry entry = data.get(i);
            response.append("*").append(2).append("\r\n");
            String id = entry.id().toString();
            response.append(formatBulkString(id));

            int valuesSize = entry.values().size() * 2;
            response.append("*").append(valuesSize).append("\r\n");
            for(String key : entry.values().keySet()) {
                response.append(formatBulkString(key));
                response.append(formatBulkString(entry.values().get(key)));
            }
        }

        return response.toString();
    }
}
