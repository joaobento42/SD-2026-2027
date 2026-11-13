package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Registo persistente de uma réplica, ordenado pelo número de sequência. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        path = Paths.get(fileName);
    }

    public synchronized void append(SensorRecord record) throws IOException {
        Objects.requireNonNull(record, "record");
        long expected = lastSeq() + 1;
        if (record.getSeq() != expected) {
            throw new IllegalArgumentException("Esperava seq " + expected
                    + ", recebido " + record.getSeq());
        }
        Files.writeString(path, record.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** Devolve todos os registos, por ordem; falha se o ficheiro tiver lacunas. */
    public synchronized List<SensorRecord> readAll() throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (!Files.exists(path)) {
            return result;
        }
        long expected = 1;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            final SensorRecord record;
            try {
                record = SensorRecord.fromLine(line);
            } catch (IllegalArgumentException e) {
                throw new IOException("Registo inválido no ficheiro " + path, e);
            }
            if (record.getSeq() != expected) {
                throw new IOException("Registos fora de ordem no ficheiro " + path
                        + ": esperado " + expected + ", encontrado " + record.getSeq());
            }
            result.add(record);
            expected++;
        }
        return result;
    }

    /** Alias legível para os registos ordenados. */
    public synchronized List<SensorRecord> orderedRecords() throws IOException {
        return readAll();
    }

    public synchronized long lastSeq() throws IOException {
        List<SensorRecord> all = readAll();
        return all.isEmpty() ? 0 : all.get(all.size() - 1).getSeq();
    }

    public synchronized SensorRecord last() throws IOException {
        List<SensorRecord> all = readAll();
        return all.isEmpty() ? null : all.get(all.size() - 1);
    }

    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (from > to) {
            return result;
        }
        for (SensorRecord record : readAll()) {
            if (record.getSeq() >= from && record.getSeq() <= to) {
                result.add(record);
            }
        }
        return result;
    }

    /**
     * Substitui a totalidade do ficheiro por um registo completo já reconciliado.
     * Usa temporário na mesma pasta e rename atómico; se não for suportado, falha
     * e deixa intacto o ficheiro anterior (não faz fallback não-atómico).
     */
    public synchronized void replaceAll(List<SensorRecord> replacement) throws IOException {
        Objects.requireNonNull(replacement, "replacement");
        StringBuilder contents = new StringBuilder();
        long expected = 1;
        for (SensorRecord record : replacement) {
            if (record == null || record.getSeq() != expected) {
                throw new IllegalArgumentException("Registo reconciliado fora de ordem: seq " + expected);
            }
            contents.append(record.toLine()).append(System.lineSeparator());
            expected++;
        }
        Path destination = path.toAbsolutePath();
        Path directory = destination.getParent();
        Path temporary = Files.createTempFile(directory, "raft-registos-", ".tmp");
        try {
            Files.writeString(temporary, contents.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Sistema de ficheiros não suporta substituição atómica: "
                    + destination, e);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    /** Remove os registos cujo seq seja maior ou igual ao indicado. */
    public synchronized void truncateFrom(long firstDivergentSeq) throws IOException {
        if (firstDivergentSeq < 1) {
            throw new IllegalArgumentException("O primeiro seq divergente tem de ser >= 1");
        }
        List<SensorRecord> keep = new ArrayList<>();
        for (SensorRecord record : readAll()) {
            if (record.getSeq() >= firstDivergentSeq) {
                break;
            }
            keep.add(record);
        }
        replaceAll(keep);
    }
}
