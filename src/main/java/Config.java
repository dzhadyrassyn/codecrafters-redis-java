import java.io.File;

public record Config(
        boolean isMaster,
        int replicaPort,
        String masterHost,
        int masterPort,
        String dir,
        String dbfilename,
        File rdbFile
) {
    public static Config fromArgs(String[] args) {
        int replicaPort = 6380;
        String masterHost = null;
        int masterPort = 6379;
        String dir = ".";
        String dbfilename = "dump.rdb";

        int port = 6379;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--replicaof" -> {
                    String[] masterInfo = args[++i].split(" ");
                    masterHost = masterInfo[0];
                    masterPort = Integer.parseInt(masterInfo[1]);
                }
                case "--dir" -> dir = args[++i];
                case "--dbfilename" -> dbfilename = args[++i];
            }
        }

        boolean isMaster = (masterHost == null);
        if (isMaster) {
            masterPort = port;
        } else {
            replicaPort = port;
        }

        File rdbFile = new File(dir + "/" + dbfilename);
        return new Config(isMaster, replicaPort, masterHost, masterPort, dir, dbfilename, rdbFile);
    }

    public int getPort() {
        return isMaster ? masterPort : replicaPort;
    }
}