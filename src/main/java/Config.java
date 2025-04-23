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
        boolean isMaster = true;
        int replicaPort = 6380;
        String masterHost = null;
        int masterPort = 6379;
        String dir = ".";
        String dbfilename = "dump.rdb";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> replicaPort = Integer.parseInt(args[++i]);
                case "--replicaof" -> {
                    isMaster = false;
                    String[] masterInfo = args[++i].split(" ");
                    masterHost = masterInfo[0];
                    masterPort = Integer.parseInt(masterInfo[1]);
                }
                case "--dir" -> dir = args[++i];
                case "--dbfilename" -> dbfilename = args[++i];
            }
        }

        File rdbFile = new File(dir + "/" + dbfilename);
        return new Config(isMaster, replicaPort, masterHost, masterPort, dir, dbfilename, rdbFile);
    }
}