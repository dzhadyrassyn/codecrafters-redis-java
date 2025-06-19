import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public sealed interface StreamableResponse extends RedisResponse permits RDBSyncResponse {

    void writeTo(OutputStream out) throws IOException;
}

record RDBSyncResponse(String header, byte[] rdbBytes) implements StreamableResponse {
    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(header.getBytes(StandardCharsets.UTF_8));
        out.write(("$" + rdbBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.write(rdbBytes);
        out.flush();
    }
}