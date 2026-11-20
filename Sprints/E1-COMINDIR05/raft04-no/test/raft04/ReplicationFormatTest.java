package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Testes sem dependências para o formato dos lotes e o registo reconciliável. */
public final class ReplicationFormatTest {
    private static int passed;

    private static void check(boolean condition, String description) {
        if (!condition) throw new AssertionError(description);
        passed++;
    }

    private static void invalid(Runnable code, String description) {
        try {
            code.run();
            throw new AssertionError(description + " (deveria rejeitar)");
        } catch (IllegalArgumentException expected) {
            passed++;
        }
    }

    private static void ioFailure(CheckedRunnable code, String description) throws Exception {
        try {
            code.run();
            throw new AssertionError(description + " (deveria falhar)");
        } catch (IOException expected) {
            passed++;
        }
    }

    private interface CheckedRunnable { void run() throws Exception; }

    private static SensorRecord record(long seq, long term, String sensor) {
        VectorClock v = VectorClock.parse(seq + ",0,0", 3);
        return new SensorRecord(seq, term, sensor, 23.5, "1700000000000", seq, v);
    }

    public static void main(String[] args) throws Exception {
        SensorRecord a = record(1, 1, "S01");
        SensorRecord b = record(2, 2, "S02");
        check("1;1;S01;23.5;1700000000000;1;1,0,0".equals(a.toLine()), "registo sete campos");
        check(SensorRecord.fromLine(a.toLine()).toLine().equals(a.toLine()), "round-trip registo");
        check(a.getTerm() == 1, "getTerm registo");
        VectorClock detached = a.getVector();
        detached.tick(0);
        check(a.getVector().get(0) == 1, "vetor não é mutável externamente");
        invalid(() -> SensorRecord.fromLine("1;S01;23.5;1700000000000;1;1,0,0"), "formato antigo recusado");
        invalid(() -> SensorRecord.fromLine("1;-1;S01;23.5;1700000000000;1;1,0,0"), "termo negativo");
        invalid(() -> SensorRecord.fromLine("0;0;S01;23.5;1700000000000;1;1,0,0"), "seq zero");
        invalid(() -> SensorRecord.fromLine("1;0;S01;NaN;1700000000000;1;1,0,0"), "NaN");
        invalid(() -> SensorRecord.fromLine("1;0;S01;23;1700000000000;-1;1,0,0"), "L negativo");
        invalid(() -> SensorRecord.fromLine("1;0;S01;23;1700000000000;1;1,-1,0"), "V negativo");
        invalid(() -> new SensorRecord(1, 1, "S;01", 21, "t", 1, new VectorClock(3)), "sensor separador");

        Batch batch = new Batch(2, 3, "127.0.0.1", 10008, 7, 2, 2, List.of(a, b));
        String wire = batch.encode();
        check(wire.startsWith("LOTE;2;3;127.0.0.1;10008;7;2;2;2\n"), "cabeçalho de 9 campos");
        Batch parsed = Batch.decode(wire, 3);
        check(wire.equals(parsed.encode()), "round-trip lote");
        check(parsed.getTerm() == 2, "termo lote");
        check(parsed.getLeader() == 3, "lider lote");
        check(parsed.getLastTerm() == 2, "ultimo termo");
        check(parsed.getHb() == 7, "hb lote");
        check(parsed.getRecords().size() == 2, "contagem registos");
        check(parsed.getRecords().get(0).getTerm() == 1, "termos antigos preservados no lote");
        check(parsed.getInc() == 2, "alias legado getInc");
        try {
            parsed.getRecords().add(a);
            throw new AssertionError("lista imutável (deveria rejeitar)");
        } catch (UnsupportedOperationException expected) {
            passed++;
        }
        check(Batch.decode(new Batch(2, 1, "localhost", 10000, 1, 2, 2, List.of()).encode(), 3)
                .getRecords().isEmpty(), "lote vazio com log existente");
        check(Batch.decode(new Batch(1, 1, "localhost", 10000, 1, 0, 0, List.of()).encode(), 3)
                .getLastTerm() == 0, "primeiro heartbeat vazio");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;1;0;0;1", 3), "n anunciado sem linha");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;1;0;0;0\n", 3), "linha a mais");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;1;0;0", 3), "cabeçalho incompleto");
        invalid(() -> Batch.decode("LOTE;-1;1;localhost;10000;1;0;0;0", 3), "lote termo negativo");
        invalid(() -> Batch.decode("LOTE;1;4;localhost;10000;1;0;0;0", 3), "líder 4 inválido");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;0;1;0;0;0", 3), "porto zero");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;0;0;0;0", 3), "hb zero");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;1;0;2;0", 3), "ultimoTermo futuro");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;1;0;1;0", 3), "vazio ultimoTermo não zero");
        invalid(() -> Batch.decode("LOTE;1;1;localhost;10000;1;1;1;1\n" + a.toLine(), 2), "V dimensão errada");
        invalid(() -> new Batch(2, 1, "localhost", 10000, 1, 2, 2, List.of(b, a)), "seq fora de ordem");
        invalid(() -> new Batch(2, 1, "localhost", 10000, 1, 2, 2, List.of(a, a)), "seq duplicado");
        invalid(() -> new Batch(1, 1, "localhost", 10000, 1, 2, 1, List.of(b)), "registo de termo futuro");
        invalid(() -> new Batch(2, 1, "localhost", 10000, 1, 2, 1, List.of(a, b)), "ultimoTermo incorreto");
        invalid(() -> new Batch(2, 1, "localhost", 10000, 1, 1, 1, List.of(b)), "seq excede ultimoSeq");

        Path dir = Files.createTempDirectory("raft-registos-test-");
        Path path = dir.resolve("no-p0-n1.txt");
        RecordFile file = new RecordFile(path.toString());
        check(file.lastSeq() == 0, "ficheiro ausente lastSeq zero");
        check(file.last() == null, "ficheiro ausente last null");
        check(file.readAll().isEmpty(), "ficheiro ausente lista vazia");
        file.append(a);
        file.append(b);
        check(file.lastSeq() == 2, "append sequencial");
        check(file.last().getTerm() == 2, "last recupera termo");
        check(file.readAll().size() == 2, "readAll 2");
        check(file.orderedRecords().get(1).getSeq() == 2, "orderedRecords");
        check(file.range(2, 2).get(0).getTerm() == 2, "range inclui termo");
        check(file.range(3, 2).isEmpty(), "range invertido vazio");
        invalid(() -> { try { file.append(b); } catch (IOException e) { throw new RuntimeException(e); } }, "append duplicado");
        invalid(() -> { try { file.append(record(4, 2, "S04")); } catch (IOException e) { throw new RuntimeException(e); } }, "append com lacuna");
        file.truncateFrom(2);
        check(file.lastSeq() == 1, "truncateFrom remove seq >= 2");
        check(Files.readString(path).equals(a.toLine() + System.lineSeparator()), "truncate preserva prefixo");
        SensorRecord bNew = record(2, 3, "S03");
        file.append(bNew);
        check(file.last().getTerm() == 3, "registo do líder substituto");
        String previous = Files.readString(path);
        invalid(() -> { try { file.replaceAll(List.of(a, a)); } catch (IOException e) { throw new RuntimeException(e); } }, "replaceAll recusa duplicados");
        check(Files.readString(path).equals(previous), "erro de validação mantém ficheiro intacto");
        file.replaceAll(List.of(a, b));
        check(file.last().getTerm() == 2, "replaceAll persistiu outro termo");
        file.truncateFrom(1);
        check(file.lastSeq() == 0, "truncateFrom(1) esvazia ficheiro");
        check(Files.exists(path), "ficheiro vazio existe");
        invalid(() -> { try { file.truncateFrom(0); } catch (IOException e) { throw new RuntimeException(e); } }, "truncateFrom(0) recusa");
        file.replaceAll(List.of(a, b));
        Files.writeString(path, a.toLine() + "\n" + record(3, 1, "S03").toLine() + "\n",
                StandardCharsets.UTF_8);
        ioFailure(file::readAll, "ficheiro com lacuna rejeitado");
        Files.writeString(path, "1;S01;23.5;1700000000000;1;1,0,0\n",
                StandardCharsets.UTF_8);
        ioFailure(file::readAll, "ficheiro COMINDIR04 exige migração");

        System.out.println("ReplicationFormatTest: " + passed + " verificações passaram");
    }
}
