import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Stream {

    private final List<StreamEntry> entries = new ArrayList<>();

    public String add(String id, String... values) {

        Map<String, String> data = new HashMap<>();
        for(int i = 0; i < values.length; i += 2) {
            data.put(values[i], values[i + 1]);
        }
        entries.add(new StreamEntry(id, data));
        return id;
    }

    public List<StreamEntry> getEntries() {
        return entries;
    }
}
