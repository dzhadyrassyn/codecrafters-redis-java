import java.util.List;
import java.util.Map;

public sealed interface SimpleResponse extends RedisResponse permits TextResponse, IntegerResponse, ErrorResponse, SimpleStringResponse, NullStringResponse, BulkStringArrayResponse, XReadResponse, XRangeResponse, BulkArrayResponse {
    String getContent();
}

record TextResponse(String data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatBulkString(data);
    }
}

record IntegerResponse(int data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatInteger(data);
    }
}

record ErrorResponse(String data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatSimpleError(data);
    }
}

record SimpleStringResponse(String data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatSimpleString(data);
    }
}

record NullStringResponse() implements SimpleResponse {

    @Override
    public String getContent() {
        return "$-1\r\n";
    }
}

record BulkStringArrayResponse(List<String> data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatBulkStringArray(data);
    }
}

record BulkArrayResponse(List<SimpleResponse> data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatBulkArray(data);
    }
}

record XReadResponse(Map<String, List<StreamEntry>> data, List<String> streams) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatXRead(data, streams);
    }
}

record XRangeResponse(List<StreamEntry> data) implements SimpleResponse {

    @Override
    public String getContent() {
        return Helper.formatXRange(data);
    }
}
