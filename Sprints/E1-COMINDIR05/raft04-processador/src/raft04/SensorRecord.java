package raft04;

import java.util.Objects;

/** Registo persistido: seq;termo;sensor;temperatura;instante;L;V. */
public class SensorRecord {
    private final long seq;
    private final long term;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature,
                        String timestamp, long lamport, VectorClock vector) {
        if (seq < 1 || term < 0 || lamport < 0 || !Double.isFinite(temperature)
                || sensorId == null || sensorId.isBlank() || hasSeparator(sensorId)
                || timestamp == null || timestamp.isBlank() || hasSeparator(timestamp)) {
            throw new IllegalArgumentException("Campos inválidos do registo");
        }
        Objects.requireNonNull(vector, "vector");
        if (vector.size() < 1) {
            throw new IllegalArgumentException("Vetor vazio");
        }
        for (int k = 0; k < vector.size(); k++) {
            if (vector.get(k) < 0) {
                throw new IllegalArgumentException("Componente negativa no vetor");
            }
        }
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();
    }

    private static boolean hasSeparator(String value) {
        return value.indexOf(';') >= 0 || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0;
    }

    /** Compatibilidade temporária com os componentes COMINDIR04; termo=0. */
    @Deprecated
    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this(seq, 0, sensorId, temperature, timestamp, lamport, vector);
    }

    public static SensorRecord now(long seq, long term, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, term, sensorId, temperature,
                Long.toString(System.currentTimeMillis()), lamport, vector);
    }

    /** Compatibilidade temporária com o PartitionLeader antigo; termo=0. */
    @Deprecated
    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return now(seq, 0, sensorId, temperature, lamport, vector);
    }

    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";"
                + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Registo nulo");
        }
        String[] fields = line.trim().split(";", -1);
        if (fields.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        long seq = Long.parseLong(fields[0]);
        long term = Long.parseLong(fields[1]);
        double temperature = Double.parseDouble(fields[3]);
        long lamport = Long.parseLong(fields[5]);
        int n = fields[6].split(",", -1).length;
        VectorClock vector = VectorClock.parse(fields[6], n);
        return new SensorRecord(seq, term, fields[2], temperature, fields[4],
                lamport, vector);
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}
