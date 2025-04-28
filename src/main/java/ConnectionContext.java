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

    @Override
    public void close() throws IOException {
        socket.close();
    }
}