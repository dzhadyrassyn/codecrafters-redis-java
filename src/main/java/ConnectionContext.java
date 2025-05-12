import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ConnectionContext implements Closeable {
    private final Socket socket;
    private final BufferedInputStream input;
    private final OutputStream output;
    private volatile long acknowledgedOffset = 0;

    public ConnectionContext(Socket socket) throws IOException {
        this.socket = socket;
        this.input = new BufferedInputStream(socket.getInputStream());
        this.output = socket.getOutputStream();
    }

    public BufferedInputStream getInput() {
        return input;
    }

    public OutputStream getOutput() {
        return output;
    }

    public Socket getSocket() {
        return socket;
    }

    public void writeLine(String line) throws IOException {
        output.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    public void write(String line) throws IOException {
        output.write(line.getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    public long getAcknowledgedOffset() {
        return acknowledgedOffset;
    }

    public void setAcknowledgedOffset(long acknowledgedOffset) {
        this.acknowledgedOffset = acknowledgedOffset;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}