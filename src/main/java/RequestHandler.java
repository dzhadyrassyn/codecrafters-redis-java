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

        RedisResponse response = dispatcher.dispatch(args, 0L, ctx);
        if (response == null) {
            return;
        }
//        System.out.println("Response in processOneCommand: " + response);
        if (response instanceof SimpleResponse simple) {
            ctx.getOutput().write(simple.getContent().getBytes(StandardCharsets.UTF_8));
            ctx.getOutput().flush();
        } else if (response instanceof StreamableResponse streamable) {
            streamable.writeTo(ctx.getOutput());
        }
    }
}
