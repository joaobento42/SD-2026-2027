package raft04;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Bloco 3: TCP real com barramento IPFS simulado. Executar em pasta vazia. */
public final class PartitionNodeBlock3Test {
    private static int passed;
    private static final class Bus {
        private final Map<String, CopyOnWriteArrayList<Consumer<String>>> channels =
                new ConcurrentHashMap<>();
        void add(String channel, Consumer<String> callback) {
            channels.computeIfAbsent(channel, key -> new CopyOnWriteArrayList<>()).add(callback);
        }
        void remove(String channel, Consumer<String> callback) {
            channels.getOrDefault(channel, new CopyOnWriteArrayList<>()).remove(callback);
        }
        int subscribers(String channel) {
            return channels.getOrDefault(channel, new CopyOnWriteArrayList<>()).size();
        }
        void publish(String channel, String message) {
            for (Consumer<String> callback : channels.getOrDefault(channel,
                    new CopyOnWriteArrayList<>())) callback.accept(message);
        }
    }
    private static final class MockPubSub extends IpfsPubSub {
        private final Bus bus;
        MockPubSub(Bus bus) { super("http://127.0.0.1:5001/api/v0"); this.bus = bus; }
        @Override public void publish(String topic, String msg) { bus.publish(topic, msg); }
        @Override public void subscribe(String topic, Consumer<String> listener)
                throws InterruptedException {
            bus.add(topic, listener);
            try { while (true) Thread.sleep(75); }
            finally { bus.remove(topic, listener); }
        }
    }
    private static void check(boolean ok, String reason) {
        if (!ok) throw new AssertionError(reason);
        passed++;
    }
    private static void until(BooleanSupplier condition, String reason) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= end) throw new AssertionError(reason);
            Thread.sleep(20);
        }
    }
    private static boolean sameFile(String a, String b) {
        try { return Files.readString(Path.of(a)).equals(Files.readString(Path.of(b))); }
        catch (Exception e) { return false; }
    }
    private static RaftState raft(PartitionNode node) throws Exception {
        Field f = PartitionNode.class.getDeclaredField("raft");
        f.setAccessible(true);
        return (RaftState) f.get(node);
    }
    private static void startElection(PartitionNode node) throws Exception {
        RaftState raft = raft(node);
        Field field = RaftState.class.getDeclaredField("deadline");
        field.setAccessible(true);
        synchronized (raft) { field.setLong(raft, System.nanoTime() - 1); }
    }
    private static void prep(int id, long stateTerm, long... terms) throws Exception {
        Files.writeString(Path.of("estado-p0-n" + id + ".txt"),
                stateTerm + ";0", StandardCharsets.UTF_8);
        RecordFile log = new RecordFile("no-p0-n" + id + ".txt");
        for (int i = 0; i < terms.length; i++) {
            int seq = i + 1;
            VectorClock vc = VectorClock.parse(seq + ",0,0", 3);
            log.append(new SensorRecord(seq, terms[i], "S" + id + "-" + seq,
                    20 + seq, Long.toString(100000L + seq), seq, vc));
        }
    }
    /** Compara seq/termo: nos prefixos comuns os payloads têm de ser iguais. */
    private static void rewritePrefix(int id, long... terms) throws Exception {
        List<SensorRecord> list = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            int seq = i + 1;
            VectorClock vc = VectorClock.parse(seq + ",0,0", 3);
            list.add(new SensorRecord(seq, terms[i], "S" + (seq <= 2 ? "comum" : id)
                    + "-" + seq, 20 + seq, Long.toString(100000L + seq), seq, vc));
        }
        new RecordFile("no-p0-n" + id + ".txt").replaceAll(list);
    }
    private static void divergence() throws Exception {
        prep(1, 2, 1, 2, 2, 2); // último bloco local começa em seq 2
        prep(2, 3, 1, 1, 3);    // o líder tem seq 2 de termo diferente
        rewritePrefix(1, 1, 2, 2, 2);
        rewritePrefix(2, 1, 1, 3);
        String oldData = Files.readString(Path.of("no-p0-n1.txt"));
        Bus bus = new Bus();
        try (PartitionNode a = new PartitionNode(0, 1, 3, "127.0.0.1", new MockPubSub(bus));
             PartitionNode b = new PartitionNode(0, 2, 3, "127.0.0.1", new MockPubSub(bus))) {
            a.start(); b.start();
            until(() -> bus.subscribers(Wire.topicElection(0)) == 2,
                    "Ambos devem escutar eleições");
            startElection(b);
            until(() -> {
                try { return raft(b).getRole() == RaftState.Role.LIDER; }
                catch (Exception e) { return false; }
            }, "n2 deveria tornar-se líder com maioria 2/3");
            until(() -> sameFile("no-p0-n1.txt", "no-p0-n2.txt"),
                    "n1 não reconciliou divergência de seq 2");
            check(!oldData.equals(Files.readString(Path.of("no-p0-n1.txt"))),
                    "O registo divergente foi substituído");
            check(new RecordFile("no-p0-n1.txt").lastSeq() == 3,
                    "n1 mantém somente 3 registos após truncagem");
            check(Files.readString(Path.of("eventos-p0-n1.txt"))
                    .contains(";TRUNCADO;p0;de=2;descartados=3;termos=2,2,2"),
                    "Evento TRUNCADO tem de incluir seq e termos descartados");
            until(() -> {
                try {
                    return Files.readString(Path.of("eventos-p0-n1.txt"))
                            .contains(";RECUPERACAO;p0;");
                } catch (Exception e) {
                    return false;
                }
            }, "Evento de recuperação não foi registado");
            check(Files.readString(Path.of("eventos-p0-n1.txt"))
                    .contains(";RECUPERACAO;p0;"), "Evento de recuperação registado");
            String hbFile = Files.readString(Path.of("hb-p0-n2.txt"));
            String[] hb = hbFile.strip().split(";", -1);
            check(hb.length == 7 && Long.parseLong(hb[0]) == raft(b).getTerm()
                    && Long.parseLong(hb[3]) == 3 && Long.parseLong(hb[6]) > 0,
                    "Cabeçalho heartbeat tem exatamente sete campos válidos");
            for (String line : Files.readAllLines(Path.of("eventos-p0-n1.txt"))) {
                check(line.split(";", -1).length >= 4 && line.matches("^[0-9]+;[^;]+;p0;.*$"),
                        "Evento deve começar com instante;evento;p0;detalhe");
            }
        }
    }
    private static void shorter() throws Exception {
        prep(1, 2, 1, 1, 1, 1);
        prep(2, 3, 1, 1, 3);
        rewritePrefix(1, 1, 1, 1, 1);
        rewritePrefix(2, 1, 1, 3);
        Bus bus = new Bus();
        try (PartitionNode a = new PartitionNode(0, 1, 3, "127.0.0.1", new MockPubSub(bus));
             PartitionNode b = new PartitionNode(0, 2, 3, "127.0.0.1", new MockPubSub(bus))) {
            a.start(); b.start();
            until(() -> bus.subscribers(Wire.topicElection(0)) == 2, "Subscrições ativas");
            startElection(b);
            until(() -> sameFile("no-p0-n1.txt", "no-p0-n2.txt"),
                    "n1 não eliminou excedente ou não corrigiu seq 3");
            check(new RecordFile("no-p0-n1.txt").lastSeq() == 3,
                    "Seguidor elimina registos posteriores ao último do líder");
            check(Files.readString(Path.of("eventos-p0-n1.txt"))
                    .contains(";TRUNCADO;p0;de=3;descartados=2;termos=1,1"),
                    "Truncagem inclui registos excedentes");
        }
    }
    private static void isolation() throws Exception {
        prep(1, 2, 1, 2, 2, 2);
        prep(2, 3, 1, 1, 3);
        rewritePrefix(1, 1, 2, 2, 2);
        rewritePrefix(2, 1, 1, 3);
        String original = Files.readString(Path.of("no-p0-n1.txt"));
        Bus bus = new Bus();
        try (PartitionNode a = new PartitionNode(0, 1, 3, "127.0.0.1", new MockPubSub(bus));
             PartitionNode b = new PartitionNode(0, 2, 3, "127.0.0.1", new MockPubSub(bus))) {
            a.start(); b.start();
            until(() -> bus.subscribers(Wire.topicElection(0)) == 2, "Subscrições ativas");
            Files.writeString(Path.of("isolar-p0-n1"), "");
            startElection(b);
            // n1 não vota isolado; não há quórum. Forçamos o termo posterior
            // apenas para testar que o isolamento protege o registo local.
            bus.publish(Wire.topicData(0), new Batch(4, 2, "127.0.0.1", 10001,
                    1, 3, 3, List.of()).encode());
            Thread.sleep(250);
            check(original.equals(Files.readString(Path.of("no-p0-n1.txt"))),
                    "Nó isolado não pode sobrescrever o ficheiro");
            Files.delete(Path.of("isolar-p0-n1"));
            // Um lote com termo superior depois de reparar rede passa a ser aceite.
            bus.publish(Wire.topicData(0), new Batch(4, 2, "127.0.0.1", 10001,
                    2, 3, 3, List.of()).encode());
            until(() -> sameFile("no-p0-n1.txt", "no-p0-n2.txt"),
                    "O nó regressado não reconciliou após remover isolamento");
            check(Files.readString(Path.of("eventos-p0-n1.txt")).contains(";TRUNCADO;p0;"),
                    "Recuperação depois de isolamento regista TRUNCADO");
        } finally { Files.deleteIfExists(Path.of("isolar-p0-n1")); }
    }
    private static void emptyLeader() throws Exception {
        prep(1, 2, 1, 1, 1);
        Bus bus = new Bus();
        try (PartitionNode a = new PartitionNode(0, 1, 3, "127.0.0.1", new MockPubSub(bus))) {
            a.start();
            until(() -> bus.subscribers(Wire.topicData(0)) == 1, "Subscrição dados");
            bus.publish(Wire.topicData(0), new Batch(4, 2, "127.0.0.1", 10001,
                    1, 0, 0, List.of()).encode());
            until(() -> {
                try { return new RecordFile("no-p0-n1.txt").lastSeq() == 0; }
                catch (Exception e) { return false; }
            }, "Leader vazio deveria provocar truncagem local completa");
            check(Files.readString(Path.of("eventos-p0-n1.txt"))
                    .contains(";TRUNCADO;p0;de=1;descartados=3;termos=1,1,1"),
                    "Leader com 0 registos causa truncagem atómica do excesso");
        }
    }
    private static void unavailableLeader() throws Exception {
        prep(1, 2, 1, 2, 2);
        String snapshot = Files.readString(Path.of("no-p0-n1.txt"));
        Bus bus = new Bus();
        try (PartitionNode a = new PartitionNode(0, 1, 3, "127.0.0.1", new MockPubSub(bus))) {
            a.start();
            until(() -> bus.subscribers(Wire.topicData(0)) == 1, "Subscrição dados");
            bus.publish(Wire.topicData(0), new Batch(4, 2, "127.0.0.1", 10001,
                    1, 4, 3, List.of()).encode());
            until(() -> {
                try { return Files.readString(Path.of("eventos-p0-n1.txt"))
                        .contains(";ERRO;p0;RECUPERACAO:"); }
                catch (Exception e) { return false; }
            }, "Falha TCP deve ser registada como ERRO");
            check(snapshot.equals(Files.readString(Path.of("no-p0-n1.txt"))),
                    "Erro de TCP não pode alterar ficheiro local");
        }
    }
    public static void main(String[] args) throws Exception {
        check(!Files.exists(Path.of("no-p0-n1.txt")), "Executar num diretório vazio");
        switch (args.length == 0 ? "divergence" : args[0]) {
            case "divergence" -> divergence();
            case "shorter" -> shorter();
            case "isolation" -> isolation();
            case "empty" -> emptyLeader();
            case "unavailable" -> unavailableLeader();
            default -> throw new IllegalArgumentException("Modo: divergence, shorter, isolation, empty, unavailable");
        }
        System.out.println("PartitionNodeBlock3Test " + (args.length == 0 ? "divergence" : args[0])
                + ": " + passed + " verificações passaram");
    }
}
