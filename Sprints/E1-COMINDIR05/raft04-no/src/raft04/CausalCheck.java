package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analisa os registos das replicas de cada particao, sem contar copias como
 * eventos separados. So usa eventos inequivocos e validos na analise causal.
 *
 * Entrada: no-p<k>-n<i>.txt (k = 0..N-1, i = 1..Wire.GRUPO).
 * Saida: vista-lamport.txt e relatorio na consola.
 */
public final class CausalCheck {
    private CausalCheck() { }

    /** Uma posicao do historico, independentemente do termo da sua versao. */
    private record Position(int partition, long seq) { }

    /** Identidade da copia de um registo, incluindo o termo em que foi escrito. */
    private record Identity(int partition, long seq, long term) { }

    private static final class Event {
        final int partition;
        final SensorRecord record;
        final VectorClock vector;

        Event(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
            this.vector = record.getVector();
        }

        String id() {
            return "p" + partition + "#" + record.getSeq() + "(t" + record.getTerm() + ")";
        }

        long lamport() { return record.getLamport(); }
    }

    /** Primeira copia observada e origens onde a mesma identidade apareceu. */
    private static final class Observed {
        final Event event;
        final String canonicalLine;
        final String firstNode;
        final Set<String> nodes = new LinkedHashSet<>();

        Observed(Event event, String node) {
            this.event = event;
            this.canonicalLine = event.record.toLine();
            this.firstNode = node;
            this.nodes.add(node);
        }
    }

    private static final class Analysis {
        final int partitions;
        final Map<Identity, Observed> byIdentity = new LinkedHashMap<>();
        final Map<Position, Map<Long, Set<String>>> termsByPosition = new LinkedHashMap<>();
        final Set<Position> invalidPositions = new HashSet<>();
        int errors;
        long copies;
        int divergentPositions;
        int contentConflicts;
        int readRecords;

        Analysis(int partitions) { this.partitions = partitions; }

        void ingest(int partition, String node, SensorRecord record) {
            readRecords++;
            Position position = new Position(partition, record.getSeq());
            Identity identity = new Identity(partition, record.getSeq(), record.getTerm());
            termsByPosition.computeIfAbsent(position, ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(record.getTerm(), ignored -> new LinkedHashSet<>())
                    .add(node);

            VectorClock vector = record.getVector();
            if (vector.size() != partitions) {
                System.out.println("ERRO " + node + " seq=" + record.getSeq()
                        + ": vetor com " + vector.size() + " posicoes; esperava " + partitions);
                errors++;
                invalidPositions.add(position);
            } else if (vector.get(partition) != record.getSeq()) {
                System.out.println("ERRO " + node + " seq=" + record.getSeq()
                        + ": V[" + partition + "]=" + vector.get(partition)
                        + " difere de seq=" + record.getSeq());
                errors++;
                invalidPositions.add(position);
            }

            Observed previous = byIdentity.get(identity);
            if (previous == null) {
                byIdentity.put(identity, new Observed(new Event(partition, record), node));
                return;
            }

            if (!previous.nodes.add(node)) {
                System.out.println("ERRO " + node + ": identidade repetida no mesmo ficheiro: "
                        + identity);
                errors++;
                invalidPositions.add(position);
            }
            if (!previous.canonicalLine.equals(record.toLine())) {
                contentConflicts++;
                System.out.println("DIVERGENCIA DE CONTEUDO p" + partition + " seq="
                        + record.getSeq() + " termo=" + record.getTerm() + ": "
                        + previous.firstNode + " e " + node + " diferem");
                errors++;
                invalidPositions.add(position);
            } else if (!previous.firstNode.equals(node)) {
                copies++;
            }
        }

        void findVersionConflicts() {
            for (Map.Entry<Position, Map<Long, Set<String>>> entry : termsByPosition.entrySet()) {
                if (entry.getValue().size() > 1) {
                    Position p = entry.getKey();
                    StringBuilder details = new StringBuilder();
                    for (Map.Entry<Long, Set<String>> version : entry.getValue().entrySet()) {
                        if (details.length() > 0) details.append("; ");
                        details.append("termo=").append(version.getKey())
                                .append(" nos=").append(version.getValue());
                    }
                    System.out.println("DIVERGENCIA DE TERMO p" + p.partition
                            + " seq=" + p.seq + ": " + details);
                    invalidPositions.add(p);
                    errors++;
                    divergentPositions++;
                }
            }
        }

        List<Event> validEvents() {
            List<Event> result = new ArrayList<>();
            for (Map.Entry<Identity, Observed> entry : byIdentity.entrySet()) {
                Identity id = entry.getKey();
                if (!invalidPositions.contains(new Position(id.partition(), id.seq()))) {
                    result.add(entry.getValue().event);
                }
            }
            return result;
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: CausalCheck <N> <pasta>");
            System.exit(2);
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            int errors = check(partitions, Paths.get(args[1]));
            if (errors > 0) {
                System.err.println("Verificacao causal concluida com " + errors + " problema(s).");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("N tem de ser um inteiro.");
            System.exit(2);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Verificacao causal falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    /**
     * Executa a verificacao, retorna o numero de problemas detetados.
     * Package-private para ser exercitado por testes sem System.exit().
     */
    static int check(int partitions, Path folder) throws IOException {
        if (partitions <= 0) {
            throw new IllegalArgumentException("N tem de ser positivo");
        }
        if (!Files.isDirectory(folder)) {
            throw new IllegalArgumentException("pasta inexistente: " + folder);
        }

        Analysis analysis = new Analysis(partitions);
        for (int k = 0; k < partitions; k++) {
            for (int i = 1; i <= Wire.GRUPO; i++) {
                String fileName = "no-p" + k + "-n" + i + ".txt";
                Path path = folder.resolve(fileName);
                if (!Files.isRegularFile(path)) {
                    System.out.println("ERRO: falta " + fileName);
                    analysis.errors++;
                    continue;
                }
                readNode(path, k, analysis);
            }
        }
        analysis.findVersionConflicts();
        List<Event> events = analysis.validEvents();
        System.out.println("REGISTOS: lidos=" + analysis.readRecords
                + "; identidades=" + analysis.byIdentity.size()
                + "; copias=" + analysis.copies
                + "; eventos validos=" + events.size()
                + "; posicoes excluidas=" + analysis.invalidPositions.size()
                + "; divergencias de termo=" + analysis.divergentPositions
                + "; divergencias de conteudo=" + analysis.contentConflicts);

        // A verificacao causal original compara apenas particoes diferentes.
        long causal = 0;
        long concurrent = 0;
        long concurrentDistinctL = 0;
        long lamportViolations = 0;
        long equalVectors = 0;
        for (int i = 0; i < events.size(); i++) {
            Event a = events.get(i);
            for (int j = i + 1; j < events.size(); j++) {
                Event b = events.get(j);
                if (a.partition == b.partition) {
                    continue;
                }
                switch (a.vector.compare(b.vector)) {
                    case BEFORE -> {
                        causal++;
                        if (a.lamport() >= b.lamport()) {
                            lamportViolations++;
                            reportViolation(a, b);
                        }
                    }
                    case AFTER -> {
                        causal++;
                        if (b.lamport() >= a.lamport()) {
                            lamportViolations++;
                            reportViolation(b, a);
                        }
                    }
                    case CONCURRENT -> {
                        concurrent++;
                        if (a.lamport() != b.lamport()) concurrentDistinctL++;
                    }
                    case EQUAL -> {
                        equalVectors++;
                        System.out.println("ERRO: " + a.id() + " e " + b.id()
                                + " tem o mesmo vetor (" + a.vector + ")");
                    }
                }
            }
        }
        analysis.errors += lamportViolations + equalVectors;

        System.out.println("PARES: " + (causal + concurrent + equalVectors)
                + "; causais=" + causal + "; concorrentes=" + concurrent);
        System.out.println("CONDICAO DE LAMPORT (a -> b => L(a) < L(b)): "
                + (lamportViolations == 0 ? "OK" : lamportViolations + " violacao(oes)"));
        System.out.println("CONCORRENTES COM L DISTINTO: " + concurrentDistinctL);

        events.sort(Comparator.comparingLong(Event::lamport)
                .thenComparingInt(e -> e.partition)
                .thenComparingLong(e -> e.record.getSeq()));
        List<String> lines = new ArrayList<>();
        for (Event e : events) {
            lines.add("(" + e.lamport() + "," + e.partition + ") " + e.id() + " "
                    + e.record.getSensorId() + " " + e.record.getTemperature()
                    + " V=(" + e.vector + ")");
        }
        Path out = folder.resolve("vista-lamport.txt");
        Files.write(out, lines, StandardCharsets.UTF_8);
        System.out.println("Ordem total por (L, k) escrita em " + out + " ("
                + lines.size() + " eventos)");
        return analysis.errors;
    }

    private static void readNode(Path path, int partition, Analysis analysis) throws IOException {
        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.out.println("ERRO ao ler " + path.getFileName() + ": " + e.getMessage());
            analysis.errors++;
            return;
        }
        int read = 0;
        for (int lineNumber = 0; lineNumber < lines.size(); lineNumber++) {
            String line = lines.get(lineNumber);
            if (line.isBlank()) continue;
            SensorRecord record;
            try {
                record = SensorRecord.fromLine(line);
            } catch (IllegalArgumentException e) {
                System.out.println("ERRO " + path.getFileName() + " linha "
                        + (lineNumber + 1) + ": " + e.getMessage());
                analysis.errors++;
                continue;
            }
            read++;
            analysis.ingest(partition, path.getFileName().toString(), record);
        }
        System.out.println(path.getFileName() + ": " + read + " registo(s)");
    }

    private static void reportViolation(Event before, Event after) {
        System.out.println("ERRO LAMPORT: " + before.id() + " -> " + after.id()
                + " mas L=" + before.lamport() + " >= L=" + after.lamport());
    }
}
