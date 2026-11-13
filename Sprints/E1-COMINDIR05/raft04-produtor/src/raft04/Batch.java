package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** LOTE;termo;lider;endereco;porto;hb;ultimoSeq;ultimoTermo;n + n linhas. */
public final class Batch {
    private final long term;
    private final int leader;
    private final String address;
    private final int port;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;
    private final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb,
                 long lastSeq, long lastTerm, List<SensorRecord> records) {
        if (term < 0 || leader < 1 || leader > 3 || address == null || address.isBlank()
                || address.indexOf(';') >= 0 || address.indexOf('\n') >= 0
                || address.indexOf('\r') >= 0 || port < 1 || port > 65535
                || hb < 1 || lastSeq < 0 || lastTerm < 0 || lastTerm > term
                || (lastSeq == 0 && lastTerm != 0) || records == null) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido");
        }
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));

        long previous = 0;
        for (SensorRecord record : this.records) {
            if (record == null || record.getSeq() <= previous
                    || record.getSeq() > lastSeq || record.getTerm() > term) {
                throw new IllegalArgumentException("Sequência/termo inválido no LOTE");
            }
            previous = record.getSeq();
        }
        // Quando o lote inclui o último registo do líder, o últimoTermo tem de coincidir.
        if (!this.records.isEmpty()) {
            SensorRecord lastIncluded = this.records.get(this.records.size() - 1);
            if (lastIncluded.getSeq() == lastSeq && lastIncluded.getTerm() != lastTerm) {
                throw new IllegalArgumentException("ultimoTermo não corresponde ao último registo");
            }
        }
    }

    /**
     * API antiga mantida unicamente para permitir compilar PartitionLeader antigo.
     * Esse líder fixo não participa numa eleição COMINDIR05 e usa termo=inc, líder=1.
     */
    @Deprecated
    public Batch(String address, int port, long inc, long hb, long lastSeq,
                 List<SensorRecord> records) {
        this(inc, 1, address, port, hb, lastSeq,
                (records != null && !records.isEmpty()
                        && records.get(records.size() - 1).getSeq() == lastSeq)
                        ? records.get(records.size() - 1).getTerm() : 0,
                records);
    }

    public String encode() {
        StringBuilder message = new StringBuilder("LOTE;")
                .append(term).append(';').append(leader).append(';')
                .append(address).append(';').append(port).append(';')
                .append(hb).append(';').append(lastSeq).append(';')
                .append(lastTerm).append(';').append(records.size());
        for (SensorRecord record : records) {
            message.append('\n').append(record.toLine());
        }
        return message.toString();
    }

    public static Batch decode(String message, int partitions) {
        if (message == null || message.isEmpty() || partitions < 1) {
            throw new IllegalArgumentException("LOTE ou número de partições inválido");
        }
        String[] lines = message.split("\\R", -1);
        String[] header = lines[0].split(";", -1);
        if (header.length != 9 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }
        long term = Long.parseLong(header[1]);
        int leader = Integer.parseInt(header[2]);
        int port = Integer.parseInt(header[4]);
        long hb = Long.parseLong(header[5]);
        long lastSeq = Long.parseLong(header[6]);
        long lastTerm = Long.parseLong(header[7]);
        int count = Integer.parseInt(header[8]);
        if (count < 0 || lines.length != count + 1) {
            throw new IllegalArgumentException("Número de registos LOTE inválido");
        }
        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            SensorRecord record = SensorRecord.fromLine(lines[i]);
            if (record.getVector().size() != partitions) {
                throw new IllegalArgumentException("Vetor do registo tem dimensão inválida");
            }
            records.add(record);
        }
        return new Batch(term, leader, header[3], port, hb, lastSeq, lastTerm, records);
    }

    public long getTerm() { return term; }
    public int getLeader() { return leader; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
    public List<SensorRecord> getRecords() { return records; }

    /** Alias transitório para clientes da COMINDIR04. No novo protocolo, inc=termo. */
    @Deprecated
    public long getInc() { return term; }
}
