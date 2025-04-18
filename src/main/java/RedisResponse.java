sealed interface RedisResponse permits TextResponse, RDBSyncResponse {}

record TextResponse(String data) implements RedisResponse {}
record RDBSyncResponse(String header, byte[] rdbBytes) implements RedisResponse {}