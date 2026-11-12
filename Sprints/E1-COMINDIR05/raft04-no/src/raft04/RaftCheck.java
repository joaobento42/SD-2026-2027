package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Verificador OFFLINE do sprint E1-COMINDIR05.
 *
 * Lê os ficheiros do ensaio, sem modificar nenhum deles. Não infere instantes de
 * arranque nem o conteúdo de registos já truncados se não tiver essas provas.
 *
 * Uso: java raft04.RaftCheck <N> <pasta> [falhas.txt]
 */
public final class RaftCheck {
    private static final int GROUP = 3;
    private RaftCheck() { }

    private record Event(long at, int partition, int node, String kind, String details) { }
    private record Heartbeat(long term, long hb, long at, long lastSeq,
                             long lastTerm, long count, long bytes, int node) { }
    private record Reading(long at, int partition, String sensor, double temperature) { }
    private record Failure(int partition, int node, long at) { }
    private record ReadKey(String sensor, double temperature) { }

    private static final class TermInfo {
        final long term;
        final Set<Integer> candidates = new TreeSet<>();
        final Set<Integer> leaders = new TreeSet<>();
        final Map<Integer, Set<Integer>> votes = new TreeMap<>();
        long firstCandidateAt = Long.MAX_VALUE;
        long firstLeaderAt = Long.MAX_VALUE;
        TermInfo(long term) { this.term = term; }
        Set<Integer> votersFor(int candidate) {
            return votes.computeIfAbsent(candidate, unused -> new TreeSet<>());
        }
    }

    private static final class NodeData {
        final int partition;
        final int node;
        final List<Event> events = new ArrayList<>();
        final List<Heartbeat> heartbeats = new ArrayList<>();
        final Map<Long, SensorRecord> records = new TreeMap<>();
        boolean recordsPresent;
        long stateTerm = -1;
        int votedFor = -1;
        NodeData(int partition, int node) { this.partition = partition; this.node = node; }
        String label() { return "p" + partition + "-n" + node; }
        long lastSeq() { return records.isEmpty() ? 0 : records.keySet().stream().mapToLong(x -> x).max().orElse(0); }
    }

    private static final class Report {
        int errors;
        int warnings;
        void error(String message) { errors++; System.out.println("ERRO: " + message); }
        void warn(String message) { warnings++; System.out.println("AVISO: " + message); }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            System.exit(2);
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            if (partitions < 1) throw new IllegalArgumentException("N tem de ser positivo");
            Path folder = Path.of(args[1]);
            Path failures = null;
            if (args.length == 3) {
                Path requested = Path.of(args[2]);
                failures = requested.isAbsolute() ? requested : folder.resolve(requested);
            }
            int errors = check(partitions, folder, failures);
            if (errors > 0) System.exit(1);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("RaftCheck falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    /** Retorna apenas o número de violações comprovadas; omissões de prova são avisos. */
    static int check(int partitions, Path folder, Path failuresFile) throws IOException {
        if (!Files.isDirectory(folder)) throw new IllegalArgumentException("Pasta inexistente: " + folder);
        Report report = new Report();
        List<NodeData> nodes = new ArrayList<>();
        List<Reading> inputs = readInputs(folder.resolve("produtor.txt"), partitions, report);
        List<Failure> failures = failuresFile == null ? List.of() : readFailures(failuresFile, partitions, report);
        System.out.println("RAFTCHECK | N=" + partitions + " | grupo=" + GROUP + " | pasta=" + folder.toAbsolutePath());
        for (int k = 0; k < partitions; k++) {
            for (int i = 1; i <= GROUP; i++) {
                NodeData data = new NodeData(k, i);
                readNode(folder, data, report);
                nodes.add(data);
            }
        }
        // A origem temporal é global: o primeiro ARRANQUE dos N * GRUPO nós,
        // não o primeiro arranque da partição que está a ser analisada.
        long globalFirstStart = nodes.stream().flatMap(n -> n.events.stream())
                .filter(e -> e.kind.equals("ARRANQUE"))
                .mapToLong(Event::at).min().orElse(Long.MAX_VALUE);
        List<Event> monitors = readMonitorFiles(folder, partitions, report);
        System.out.println("MONITORES: " + monitors.size() + " eventos em monitor-*.txt");
        for (int k = 0; k < partitions; k++) {
            List<NodeData> group = new ArrayList<>();
            for (NodeData node : nodes) if (node.partition == k) group.add(node);
            analyzePartition(k, group, inputs, failures, monitors, globalFirstStart, report);
        }
        System.out.println("RESUMO: " + report.errors + " erro(s) comprovados, "
                + report.warnings + " aviso(s)/limitações de observabilidade.");
        return report.errors;
    }

    private static void readNode(Path folder, NodeData node, Report report) throws IOException {
        Path eventsPath = folder.resolve("eventos-" + node.label() + ".txt");
        Path hbPath = folder.resolve("hb-" + node.label() + ".txt");
        Path recordsPath = folder.resolve("no-" + node.label() + ".txt");
        Path statePath = folder.resolve("estado-" + node.label() + ".txt");
        if (Files.exists(eventsPath)) {
            int lineNumber = 0;
            for (String line : Files.readAllLines(eventsPath, StandardCharsets.UTF_8)) {
                lineNumber++;
                if (line.isBlank()) continue;
                String[] f = line.split(";", 4);
                try {
                    if (f.length != 4 || !f[2].equals("p" + node.partition)) throw new IllegalArgumentException("campos/partição");
                    node.events.add(new Event(Long.parseLong(f[0]), node.partition, node.node, f[1], f[3]));
                } catch (IllegalArgumentException e) {
                    report.error(eventsPath.getFileName() + ": linha " + lineNumber + " inválida (" + e.getMessage() + ")");
                }
            }
        } else report.warn("Falta " + eventsPath.getFileName());
        if (Files.exists(hbPath)) {
            int lineNumber = 0;
            long previousHeartbeatTerm = -1;
            for (String line : Files.readAllLines(hbPath, StandardCharsets.UTF_8)) {
                lineNumber++;
                if (line.isBlank()) continue;
                String[] f = line.split(";", -1);
                try {
                    if (f.length != 7) throw new IllegalArgumentException("esperados 7 campos");
                    Heartbeat hb = new Heartbeat(Long.parseLong(f[0]), Long.parseLong(f[1]),
                            Long.parseLong(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
                            Long.parseLong(f[5]), Long.parseLong(f[6]), node.node);
                    if (hb.term < 0 || hb.hb < 1 || hb.lastSeq < 0 || hb.lastTerm < 0
                            || hb.count < 0 || hb.bytes < 0) throw new IllegalArgumentException("valor negativo/inválido");
                    if (hb.term < previousHeartbeatTerm) {
                        report.error(hbPath.getFileName() + ": termo diminuiu de "
                                + previousHeartbeatTerm + " para " + hb.term);
                    }
                    previousHeartbeatTerm = Math.max(previousHeartbeatTerm, hb.term);
                    node.heartbeats.add(hb);
                } catch (IllegalArgumentException e) {
                    report.error(hbPath.getFileName() + ": linha " + lineNumber + " inválida (" + e.getMessage() + ")");
                }
            }
        } else report.warn("Falta " + hbPath.getFileName());
        if (Files.exists(recordsPath)) {
            node.recordsPresent = true;
            int lineNumber = 0;
            for (String line : Files.readAllLines(recordsPath, StandardCharsets.UTF_8)) {
                lineNumber++;
                if (line.isBlank()) continue;
                try {
                    SensorRecord record = SensorRecord.fromLine(line);
                    if (node.records.putIfAbsent(record.getSeq(), record) != null) {
                        report.error(recordsPath.getFileName() + ": seq repetido " + record.getSeq());
                    }
                } catch (IllegalArgumentException e) {
                    report.error(recordsPath.getFileName() + ": registo inválido linha " + lineNumber + " (" + e.getMessage() + ")");
                }
            }
        } else report.warn("Falta " + recordsPath.getFileName());
        if (Files.exists(statePath)) {
            String[] f = Files.readString(statePath, StandardCharsets.UTF_8).trim().split(";", -1);
            try {
                if (f.length != 2) throw new IllegalArgumentException("esperados 2 campos");
                node.stateTerm = Long.parseLong(f[0]);
                node.votedFor = Integer.parseInt(f[1]);
                if (node.stateTerm < 0 || node.votedFor < 0 || node.votedFor > GROUP)
                    throw new IllegalArgumentException("termo/voto inválido");
            } catch (IllegalArgumentException e) {
                report.error(statePath.getFileName() + ": " + e.getMessage());
            }
        } else report.warn("Falta " + statePath.getFileName());
    }

    private static List<Reading> readInputs(Path path, int partitions, Report report) throws IOException {
        if (!Files.exists(path)) { report.warn("Falta produtor.txt; não é possível medir leituras perdidas"); return List.of(); }
        List<Reading> result = new ArrayList<>();
        int lineNumber = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) continue;
            String[] f = line.split(";", -1);
            try {
                if (f.length != 4 || !f[1].matches("p\\d+")) throw new IllegalArgumentException("campos");
                int k = Integer.parseInt(f[1].substring(1));
                if (k < 0 || k >= partitions) throw new IllegalArgumentException("partição inválida");
                double temp = Double.parseDouble(f[3]);
                if (!Double.isFinite(temp)) throw new IllegalArgumentException("temperatura inválida");
                result.add(new Reading(Long.parseLong(f[0]), k, f[2], temp));
            } catch (IllegalArgumentException e) {
                report.error("produtor.txt linha " + lineNumber + " inválida: " + e.getMessage());
            }
        }
        return result;
    }

    private static List<Failure> readFailures(Path path, int partitions, Report report) throws IOException {
        if (!Files.exists(path)) { report.warn("Falta " + path + "; não é possível medir indisponibilidades"); return List.of(); }
        List<Failure> result = new ArrayList<>();
        int lineNumber = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) continue;
            String[] f = line.split(";", -1);
            try {
                if (f.length != 3 || !f[0].matches("p\\d+") || !f[1].matches("n\\d+"))
                    throw new IllegalArgumentException("campos");
                int k = Integer.parseInt(f[0].substring(1));
                int i = Integer.parseInt(f[1].substring(1));
                if (k < 0 || k >= partitions || i < 1 || i > GROUP) throw new IllegalArgumentException("partição/nó inválido");
                result.add(new Failure(k, i, Long.parseLong(f[2])));
            } catch (IllegalArgumentException e) {
                report.error(path.getFileName() + " linha " + lineNumber + ": " + e.getMessage());
            }
        }
        return result;
    }

    private static List<Event> readMonitorFiles(Path folder, int partitions, Report report) throws IOException {
        List<Event> result = new ArrayList<>();
        for (String name : List.of("monitor-produtor.txt", "monitor-processador.txt")) {
            Path path = folder.resolve(name);
            if (!Files.exists(path)) { report.warn("Falta " + name); continue; }
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                String[] f = line.split(";", 4);
                try {
                    if (f.length != 4 || !f[2].matches("p\\d+")) throw new IllegalArgumentException("campos");
                    int k = Integer.parseInt(f[2].substring(1));
                    if (k < 0 || k >= partitions) throw new IllegalArgumentException("partição");
                    result.add(new Event(Long.parseLong(f[0]), k, 0, f[1], f[3]));
                } catch (IllegalArgumentException e) {
                    report.error(name + ": linha inválida: " + e.getMessage());
                }
            }
        }
        return result;
    }

    private static void analyzePartition(int k, List<NodeData> group, List<Reading> inputs,
                                         List<Failure> failures, List<Event> monitors,
                                         long globalFirstStart, Report report) {
        System.out.println("\n=== PARTIÇÃO p" + k + " ===");
        Map<Long, TermInfo> terms = new TreeMap<>();
        List<Event> all = new ArrayList<>();
        List<Heartbeat> heartbeats = new ArrayList<>();
        for (NodeData node : group) {
            all.addAll(node.events);
            heartbeats.addAll(node.heartbeats);
        }
        all.sort(Comparator.comparingLong(Event::at).thenComparingInt(Event::node));
        heartbeats.sort(Comparator.comparingLong(Heartbeat::at));

        for (NodeData node : group) {
            long lastObservedTerm = -1;
            // A ordem do ficheiro reflete a sequência observada pelo nó. Não ordenar
            // cronologicamente aqui: instantes iguais podem ocorrer no mesmo ms.
            for (Event event : node.events) {
                OptionalLong eventTerm = termFromEvent(event);
                if (eventTerm.isPresent()) {
                    long t = eventTerm.getAsLong();
                    if (t < lastObservedTerm) report.error(node.label() + ": termo diminuiu em " + event.kind + " (" + lastObservedTerm + "->" + t + ")");
                    lastObservedTerm = Math.max(lastObservedTerm, t);
                }
            }
            if (node.stateTerm >= 0 && lastObservedTerm > node.stateTerm) {
                report.error(node.label() + ": estado persistido termo=" + node.stateTerm
                        + " é inferior ao termo " + lastObservedTerm + " observado nos eventos");
            }
        }
        for (Event event : all) {
            // Mesmo sem candidatura ou liderança observada, TERMO prova que esta
            // época existiu. Não a omitir do relatório eleitoral.
            OptionalLong observedTerm = termFromEvent(event);
            if (observedTerm.isPresent()) {
                terms.computeIfAbsent(observedTerm.getAsLong(), TermInfo::new);
            }
            if (event.kind.equals("CANDIDATO")) {
                OptionalLong t = propertyLong(event.details, "termo");
                if (t.isPresent()) {
                    TermInfo info = terms.computeIfAbsent(t.getAsLong(), TermInfo::new);
                    info.candidates.add(event.node);
                    info.votersFor(event.node).add(event.node);
                    info.firstCandidateAt = Math.min(info.firstCandidateAt, event.at);
                }
            } else if (event.kind.equals("VOTO")) {
                OptionalLong t = propertyLong(event.details, "termo");
                int candidate = propertyNode(event.details, "candidato");
                int voter = propertyNode(event.details, "eleitor");
                if (t.isPresent() && candidate > 0 && voter > 0 && event.details.contains("concedido")) {
                    if (candidate > GROUP || voter > GROUP || voter != event.node) {
                        report.error("p" + k + ": voto concedido com eleitor/candidato inválido em "
                                + "eventos-p" + k + "-n" + event.node + ".txt");
                    } else {
                        terms.computeIfAbsent(t.getAsLong(), TermInfo::new).votersFor(candidate).add(voter);
                    }
                }
            } else if (event.kind.equals("LIDER")) {
                OptionalLong t = propertyLong(event.details, "termo");
                if (t.isPresent()) {
                    TermInfo info = terms.computeIfAbsent(t.getAsLong(), TermInfo::new);
                    info.leaders.add(event.node);
                    info.firstLeaderAt = Math.min(info.firstLeaderAt, event.at);
                }
            }
        }
        for (Heartbeat hb : heartbeats) terms.computeIfAbsent(hb.term, TermInfo::new);
        int multipleLeaders = 0;
        int splitVotes = 0;
        for (TermInfo info : terms.values()) {
            if (info.leaders.size() > 1) {
                multipleLeaders++;
                report.error("p" + k + " termo " + info.term + ": mais de um líder " + info.leaders);
            }
            StringBuilder candidates = new StringBuilder();
            for (int candidate : info.candidates) {
                if (candidates.length() > 0) candidates.append(", ");
                candidates.append("n").append(candidate).append("=")
                        .append(info.votersFor(candidate).size()).append(" voto(s)")
                        .append(info.votersFor(candidate));
            }
            String duration = info.firstLeaderAt != Long.MAX_VALUE && info.firstCandidateAt != Long.MAX_VALUE
                    ? (info.firstLeaderAt - info.firstCandidateAt) + " ms" : "N/D";
            String conclusion;
            if (!info.leaders.isEmpty()) conclusion = "líder=" + info.leaders;
            else {
                // Indicação pedida para termos sem LIDER; não implica que um
                // empate de votos tenha sido comprovado.
                conclusion = "VOTO DIVIDIDO (sem LIDER observado; causa não comprovada)";
                splitVotes++;
            }
            System.out.println("TERMO " + info.term + " | candidatos: " + (candidates.length() == 0 ? "não observados" : candidates)
                    + " | " + conclusion + " | duração eleição=" + duration);
        }
        System.out.println("SEGURANÇA: termos com múltiplos líderes=" + multipleLeaders
                + " | termos sem líder (indicação VOTO DIVIDIDO)=" + splitVotes);
        if (terms.isEmpty()) report.warn("p" + k + ": não foram encontrados termos de eleição");

        long firstLeader = all.stream().filter(e -> e.kind.equals("LIDER"))
                .mapToLong(Event::at).min().orElse(Long.MAX_VALUE);
        if (firstLeader == Long.MAX_VALUE) System.out.println("PRIMEIRO LÍDER: não observado");
        else if (globalFirstStart != Long.MAX_VALUE && firstLeader >= globalFirstStart)
            System.out.println("PRIMEIRO LÍDER: " + (firstLeader - globalFirstStart)
                    + " ms desde o primeiro ARRANQUE global");
        else {
            System.out.println("PRIMEIRO LÍDER: N/D (sem ARRANQUE global válido anterior); primeiro LIDER em " + firstLeader);
            report.warn("p" + k + ": não há ARRANQUE global válido antes do primeiro líder");
        }

        reportFailures(k, all, heartbeats, inputs, failures, report);
        reportReadLoss(k, group, inputs, report);
        reportTruncation(k, all, report);
        reportFinalAgreement(k, group, report);
        long perPartitionMonitor = monitors.stream().filter(e -> e.partition == k).count();
        System.out.println("EVENTOS DOS MONITORES: " + perPartitionMonitor);
    }

    private static OptionalLong termFromEvent(Event event) {
        if (event.kind.equals("TERMO")) return propertyLong(event.details, "para");
        if (event.kind.equals("CANDIDATO") || event.kind.equals("LIDER")
                || event.kind.equals("VOTO") || event.kind.equals("NOVO_LIDER")) {
            return propertyLong(event.details, "termo");
        }
        return OptionalLong.empty();
    }

    private static OptionalLong propertyLong(String details, String key) {
        for (String token : details.split(";")) {
            if (token.startsWith(key + "=")) {
                try { return OptionalLong.of(Long.parseLong(token.substring(key.length() + 1))); }
                catch (NumberFormatException ignored) { return OptionalLong.empty(); }
            }
        }
        return OptionalLong.empty();
    }

    private static int propertyNode(String details, String key) {
        for (String token : details.split(";")) {
            if (token.startsWith(key + "=n")) {
                try { return Integer.parseInt(token.substring(key.length() + 2)); }
                catch (NumberFormatException ignored) { return 0; }
            }
        }
        return 0;
    }

    /**
     * Último líder observável no termo mais alto até ao instante da falha.
     * Se não houver LIDER para esse termo, se houver mais do que um líder,
     * ou se este se tiver demitido, a identidade fica indeterminada.
     * A ausência de eventos não constitui prova de disponibilidade contínua.
     */
    private static Event observedLeaderAt(List<Event> events, long at) {
        long highestTerm = -1;
        for (Event e : events) {
            if (e.at > at) continue;
            OptionalLong term = termFromEvent(e);
            if (term.isPresent()) highestTerm = Math.max(highestTerm, term.getAsLong());
        }
        Map<Integer, Event> leaders = new HashMap<>();
        for (Event e : events) {
            if (e.at > at || !e.kind.equals("LIDER")) continue;
            if (propertyLong(e.details, "termo").orElse(-1) != highestTerm) continue;
            Event old = leaders.get(e.node);
            if (old == null || e.at > old.at) leaders.put(e.node, e);
        }
        // Uma demissão posterior à liderança invalida a observação.
        final long observedTerm = highestTerm;
        leaders.values().removeIf(leader -> events.stream().anyMatch(e ->
                e.node == leader.node && e.at >= leader.at && e.at <= at
                        && e.kind.equals("DEMISSAO")
                        && propertyLong(e.details, "termo").orElse(-1) == observedTerm));
        return leaders.size() == 1 ? leaders.values().iterator().next() : null;
    }

    private static void reportFailures(int k, List<Event> events, List<Heartbeat> heartbeats,
                                       List<Reading> inputs, List<Failure> failures, Report report) {
        for (Failure failure : failures) {
            if (failure.partition != k) continue;
            Event leader = observedLeaderAt(events, failure.at);
            if (leader == null) {
                System.out.println("FALHA p" + k + "-n" + failure.node + " @" + failure.at
                        + ": líder em funções não determinável pelos eventos; indisponibilidade N/D");
                continue;
            }
            long term = propertyLong(leader.details, "termo").orElse(-1);
            if (failure.node != leader.node) {
                System.out.println("FALHA p" + k + "-n" + failure.node + " @" + failure.at
                        + " | falha de seguidor; último líder observado=n" + leader.node
                        + " termo=" + term
                        + " | indisponibilidade da partição não demonstrada (N/D)");
                continue;
            }
            Heartbeat recovered = null;
            for (Heartbeat hb : heartbeats) {
                if (hb.at > failure.at && hb.term > term
                        && (recovered == null || hb.at < recovered.at)) recovered = hb;
            }
            if (recovered == null) {
                System.out.println("FALHA p" + k + "-n" + failure.node
                        + " | falha do líder observado (termo " + term
                        + "); não foi observado LOTE de termo superior; indisponibilidade N/D");
                continue;
            }
            final long recoveredAt = recovered.at;
            long count = inputs.stream().filter(r -> r.partition == k
                    && r.at >= failure.at && r.at <= recoveredAt).count();
            System.out.println("FALHA p" + k + "-n" + failure.node + " @" + failure.at
                    + " | falha do líder observado (termo " + term + ")"
                    + " | primeiro LOTE de termo superior=" + recovered.at
                    + " (termo " + recovered.term + ", n" + recovered.node + ")"
                    + " | indisponibilidade=" + (recovered.at - failure.at) + " ms"
                    + " | IN enviados no intervalo=" + count);
        }
    }

    private static void reportReadLoss(int k, List<NodeData> group, List<Reading> inputs, Report report) {
        List<Reading> sent = new ArrayList<>();
        for (Reading r : inputs) if (r.partition == k) sent.add(r);
        if (inputs.isEmpty()) { System.out.println("LEITURAS PERDIDAS: N/D (sem entradas do produtor)"); return; }
        Map<ReadKey, Integer> maximumObserved = new HashMap<>();
        for (NodeData node : group) {
            Map<ReadKey, Integer> count = new HashMap<>();
            for (SensorRecord record : node.records.values()) {
                ReadKey key = new ReadKey(record.getSensorId(), record.getTemperature());
                count.merge(key, 1, Integer::sum);
            }
            for (var entry : count.entrySet()) maximumObserved.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        int missing = 0;
        List<String> examples = new ArrayList<>();
        for (Reading r : sent) {
            ReadKey key = new ReadKey(r.sensor, r.temperature);
            int available = maximumObserved.getOrDefault(key, 0);
            if (available > 0) maximumObserved.put(key, available - 1);
            else { missing++; if (examples.size() < 5) examples.add(r.sensor + "@" + r.at); }
        }
        System.out.println("LEITURAS PERDIDAS: " + missing + " / " + sent.size()
                + " (correspondência por sensor+temperatura; sem ID único de IN)"
                + (examples.isEmpty() ? "" : " | exemplos=" + examples));
        if (group.stream().anyMatch(n -> !n.recordsPresent)) {
            report.warn("p" + k + ": faltam ficheiros de nós; contagem de leituras perdidas pode ser sobrestimada");
        }
    }

    private static void reportTruncation(int k, List<Event> events, Report report) {
        int operations = 0;
        long discarded = 0;
        for (Event e : events) if (e.kind.equals("TRUNCADO")) {
            operations++;
            long n = propertyLong(e.details, "descartados").orElse(-1);
            if (n < 0) report.error("TRUNCADO sem descartados válido em p" + k + "-n" + e.node);
            else discarded += n;
            System.out.println("TRUNCADO p" + k + "-n" + e.node + " @" + e.at + " " + e.details);
        }
        System.out.println("TRUNCAGEM: operações=" + operations + " | registos descartados=" + discarded);
        if (operations > 0) {
            System.out.println("TRUNCADOS REAPARECIDOS COM OUTRO seq: N/D; o evento TRUNCADO só guarda seq/quantidade/termos, não o conteúdo descartado");
            report.warn("p" + k + ": não é possível identificar registos truncados com outro seq sem guardar o conteúdo anterior");
        }
    }

    private static void reportFinalAgreement(int k, List<NodeData> group, Report report) {
        long minLast = group.stream().mapToLong(NodeData::lastSeq).min().orElse(0);
        long maxLast = group.stream().mapToLong(NodeData::lastSeq).max().orElse(0);
        List<Long> divergent = new ArrayList<>();
        for (long seq = 1; seq <= minLast; seq++) {
            Set<String> versions = new HashSet<>();
            for (NodeData node : group) {
                SensorRecord record = node.records.get(seq);
                versions.add(record == null ? "<em falta>" : record.toLine());
            }
            if (versions.size() > 1) {
                divergent.add(seq);
                report.error("p" + k + " seq=" + seq + ": os três nós têm registos diferentes");
            }
        }
        System.out.println("COINCIDÊNCIA FINAL: seq divergentes até minUltimoSeq=" + minLast
                + ": " + divergent + " | últimos seq=" + group.stream().map(NodeData::lastSeq).toList()
                + " | maiorUltimoSeq=" + maxLast);
    }
}
