import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RequestHandler {

    private final Config config;
    private final CommandDispatcher dispatcher;

    public RequestHandler(Config config) {
        this.config = config;
        this.dispatcher = new CommandDispatcher(config);
    }

    public void handleWithPreloadedCommand(ConnectionContext ctx, String[] preloadedCommand) {
        System.out.println("Method handleWithPreloadedCommand is called");
        try {
            processOneCommand(ctx, preloadedCommand);

            handle(ctx);
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        }
    }

    public void handle(ConnectionContext ctx) throws IOException {
        System.out.println("Method handle is called");
        BufferedInputStream input = ctx.getInput();

        while (true) {
            String[] args = Helper.parseRespCommand(input);
            if (args == null) {
                System.out.println("Client disconnected");
                break;
            }

            processOneCommand(ctx, args);
        }
    }

    private void processOneCommand(ConnectionContext ctx, String[] args) throws IOException {
        System.out.println("Method processOneCommand is called");
        RedisResponse response = dispatcher.dispatch(args);
        if (response == null) {
            return;
        }
        if (response instanceof TextResponse(String data)) {
            ctx.getOutput().write(data.getBytes(StandardCharsets.UTF_8));
            ctx.getOutput().flush();
        } else if (response instanceof RDBSyncResponse(String header, byte[] rdbBytes)) {
            ctx.getOutput().write(header.getBytes(StandardCharsets.UTF_8));
            ctx.getOutput().write(("$" + rdbBytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            ctx.getOutput().write(rdbBytes);
            ctx.getOutput().flush();
        }
    }

}
