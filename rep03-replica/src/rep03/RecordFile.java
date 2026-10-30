package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;
    private final int n;

    public RecordFile(String fileName) {
        this(fileName, 0);
    }

    public RecordFile(String fileName, int n) {
        this.path = Paths.get(fileName);
        this.n = n;
    }

    public String name() {
        return path.toString();
    }

    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized List<SensorRecord> readAll() throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                out.add(parse(line));
            }
        }
        return out;
    }

    public synchronized SensorRecord last() throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        for (int i = lines.size() - 1; i >= 0; i--) {
            if (!lines.get(i).isBlank()) {
                return parse(lines.get(i));
            }
        }
        return null;
    }

    public synchronized long lastSeq() throws IOException {
        SensorRecord r = last();
        return r == null ? 0 : r.getSeq();
    }

    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        for (SensorRecord r : readAll()) {
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }

    private SensorRecord parse(String line) {
        return n > 0 ? SensorRecord.fromLine(line, n) : SensorRecord.fromLine(line);
    }
}
