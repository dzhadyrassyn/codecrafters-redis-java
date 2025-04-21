public record Config(
        boolean isMaster,
        int replicaPort,
        String masterHost,
        int masterPort,
        String dir,
        String dbfilename
) {
    public static Config fromArgs(String[] args) {
        boolean isMaster = true;
        int replicaPort = 6379;
        String masterHost = null;
        int masterPort = 6379;
        String dir = ".";
        String dbfilename = "dump.rdb";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> replicaPort = Integer.parseInt(args[++i]);
                case "replicaof" -> {
                    isMaster = false;
                    masterHost = args[++i];
                    masterPort = Integer.parseInt(args[++i]);
                }
                case "--dir" -> dir = args[++i];
                case "--dbfilename" -> dbfilename = args[++i];
            }
        }

        return new Config(isMaster, replicaPort, masterHost, masterPort, dir, dbfilename);
    }
}