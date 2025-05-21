import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RequestHandler {

    private final Config config;
    private final CommandDispatcher dispatcher;

    public RequestHandler(Config config) {
        this.config = config;
        this.dispatcher = new CommandDispatcher(config);
    }

    public void handleWithPreloadedCommand(ConnectionContext ctx, String[] preloadedCommand) {

        try {
            processOneCommand(ctx, preloadedCommand);

            handle(ctx);
        } catch (IOException e) {
            System.err.println("Error handling client in handleWithPreloadedCommand: " + e.getMessage());
        }
    }

    public void handle(ConnectionContext ctx) throws IOException {

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

        System.out.println("processOneCommand command: " + Arrays.toString(args));
        RedisResponse response = dispatcher.dispatch(args);
        System.out.println("is the response here : " + response);
        if (response == null) {
            return;
        }
        System.out.println("Response in processOneCommand: " + response);
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
