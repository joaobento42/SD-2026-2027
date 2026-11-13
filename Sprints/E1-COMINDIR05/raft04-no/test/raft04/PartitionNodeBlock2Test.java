package raft04;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Ensaio em memória do Bloco 2: sem precisar de Kubo real. */
public class PartitionNodeBlock2Test {
    private static int passed = 0;
    private static final class Bus {
        private final Map<String, CopyOnWriteArrayList<Consumer<String>>> listeners =
                new ConcurrentHashMap<>();
        int subscribers(String topic) {
            return listeners.getOrDefault(topic, new CopyOnWriteArrayList<>()).size();
        }
        void add(String topic, Consumer<String> listener) {
            listeners.computeIfAbsent(topic, ignored -> new CopyOnWriteArrayList<>()).add(listener);
        }
        void remove(String topic, Consumer<String> listener) {
            listeners.getOrDefault(topic, new CopyOnWriteArrayList<>()).remove(listener);
        }
        void publish(String topic, String message) {
            for (Consumer<String> callback : listeners.getOrDefault(topic,
                    new CopyOnWriteArrayList<>())) callback.accept(message);
        }
    }
    private static final class MockPubSub extends IpfsPubSub {
        private final Bus bus;
        MockPubSub(Bus bus) { super("http://127.0.0.1:5001/api/v0"); this.bus = bus; }
        @Override public void publish(String topic, String message) { bus.publish(topic, message); }
        @Override public void subscribe(String topic, Consumer<String> onMessage)
                throws InterruptedException {
            bus.add(topic, onMessage);
            try {
                while (true) Thread.sleep(100L);
            } finally {
                bus.remove(topic, onMessage);
            }
        }
    }
    private static void check(boolean ok, String info) {
        if (!ok) throw new AssertionError(info);
        passed++;
    }
    private static RaftState raft(PartitionNode node) throws Exception {
        Field f = PartitionNode.class.getDeclaredField("raft");
        f.setAccessible(true);
        return (RaftState) f.get(node);
    }
    private static void forceDeadline(PartitionNode node) throws Exception {
        RaftState s = raft(node);
        Field f = RaftState.class.getDeclaredField("deadline");
        f.setAccessible(true);
        synchronized (s) { f.setLong(s, System.nanoTime() - 1); }
    }
    private static void delayDeadline(PartitionNode node, long seconds) throws Exception {
        RaftState s = raft(node);
        Field f = RaftState.class.getDeclaredField("deadline");
        f.setAccessible(true);
        synchronized (s) { f.setLong(s, System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)); }
    }
    private static void invokeSendBatch(PartitionNode node) throws Exception {
        var f = PartitionNode.class.getDeclaredMethod("sendBatch");
        f.setAccessible(true);
        f.invoke(node);
    }
    private static void waitUntil(java.util.function.BooleanSupplier condition,
                                  String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError(message);
            Thread.sleep(25L);
        }
    }
    public static void main(String[] args) throws Exception {
        check(!Files.exists(Path.of("no-p0-n1.txt")), "Teste tem de correr em pasta temporária");
        Bus bus = new Bus();
        PartitionNode[] nodes = new PartitionNode[3];
        try {
            for (int i = 0; i < 3; i++) {
                nodes[i] = new PartitionNode(0, i + 1, 3, "127.0.0.1", new MockPubSub(bus));
                nodes[i].start();
            }
            waitUntil(() -> bus.subscribers(Wire.topicIn(0)) == 3,
                    "3 assinaturas IN não estão ativas");
            waitUntil(() -> bus.subscribers(Wire.topicData(0)) == 3,
                    "3 assinaturas dados não estão ativas");
            waitUntil(() -> bus.subscribers(Wire.topicElection(0)) == 3,
                    "3 assinaturas eleição não estão ativas");
            passed += 3;

            forceDeadline(nodes[0]);
            waitUntil(() -> raftUnchecked(nodes[0]).getRole() == RaftState.Role.LIDER,
                    "n1 não obteve maioria");
            check(raft(nodes[0]).getTerm() == 1, "term do primeiro líder = 1");
            waitUntil(() -> raftUnchecked(nodes[1]).getKnownLeader() == 1
                            && raftUnchecked(nodes[2]).getKnownLeader() == 1,
                    "seguidores não reconheceram líder 1");
            check(raft(nodes[1]).getKnownLeader() == 1, "n2 reconhece líder 1");
            check(raft(nodes[2]).getKnownLeader() == 1, "n3 reconhece líder 1");

            String sensor = "B";
            check(new Partitioner(3).of(sensor) == 0, "Sensor deve pertencer a p0");
            bus.publish(Wire.topicIn(0), Wire.in(sensor, 25.5, 0, new VectorClock(3)));
            check(Files.readAllLines(Path.of("no-p0-n1.txt")).size() == 1,
                    "líder persiste IN");
            check(!Files.exists(Path.of("no-p0-n2.txt")), "seguidor não processa IN diretamente");
            invokeSendBatch(nodes[0]);
            check(Files.readAllLines(Path.of("no-p0-n2.txt")).size() == 1,
                    "n2 aplica LOTE do líder");
            check(Files.readAllLines(Path.of("no-p0-n3.txt")).size() == 1,
                    "n3 aplica LOTE do líder");
            SensorRecord record = SensorRecord.fromLine(Files.readAllLines(
                    Path.of("no-p0-n2.txt")).get(0));
            check(record.getTerm() == 1 && record.getVector().get(0) == 1,
                    "termo e V[k] preservados");

            // O isolamento do líder também bloqueia saída de dados,
            // mas NÃO bloqueia o tópico de IN (requisito da ficha).
            Files.writeString(Path.of("isolar-p0-n1"), "");
            bus.publish(Wire.topicIn(0), Wire.in(sensor, 24.0, 0, new VectorClock(3)));
            check(Files.readAllLines(Path.of("no-p0-n1.txt")).size() == 2,
                    "líder isolado ainda recebe IN");
            invokeSendBatch(nodes[0]);
            check(Files.readAllLines(Path.of("no-p0-n2.txt")).size() == 1,
                    "líder isolado não publica LOTE");
            Files.delete(Path.of("isolar-p0-n1"));
            invokeSendBatch(nodes[0]);
            check(Files.readAllLines(Path.of("no-p0-n2.txt")).size() == 2,
                    "lote enviado depois de retirar isolamento do líder");

            Files.writeString(Path.of("isolar-p0-n2"), "");
            bus.publish(Wire.topicIn(0), Wire.in(sensor, 26.5, 0, new VectorClock(3)));
            invokeSendBatch(nodes[0]);
            check(Files.readAllLines(Path.of("no-p0-n2.txt")).size() == 2,
                    "n2 isolado ignora dados");
            check(Files.readAllLines(Path.of("no-p0-n3.txt")).size() == 3,
                    "n3 recebe terceiro registo");
            forceDeadline(nodes[1]);
            Thread.sleep(350L);
            check(raft(nodes[1]).getRole() != RaftState.Role.LIDER,
                    "nó isolado não pode ser eleito");
            Files.delete(Path.of("isolar-p0-n2"));
            // O relógio de n2 ficou expirado durante o isolamento; aqui
            // espaçamos a candidatura apenas para ensaiar o caso sem empate.
            delayDeadline(nodes[1], 5);

            nodes[0].close();
            // n3 possui o log mais atualizado e pode ganhar uma eleição com n2.
            forceDeadline(nodes[2]);
            waitUntil(() -> raftUnchecked(nodes[2]).getRole() == RaftState.Role.LIDER,
                    "n3 não ganhou após falha de n1");
            check(raft(nodes[2]).getTerm() >= 2, "novo mandato é de termo superior");
            waitUntil(() -> raftUnchecked(nodes[1]).getKnownLeader() == 3,
                    "n2 não reconheceu líder 3");
            check(raft(nodes[1]).getKnownLeader() == 3, "n2 reconhece novo líder");
            waitUntil(() -> {
                try { return Files.readAllLines(Path.of("no-p0-n2.txt")).size() == 3; }
                catch (Exception e) { return false; }
            }, "recuperação de lacuna TCP não foi concluída");
            check(Files.readAllLines(Path.of("no-p0-n2.txt")).size() == 3,
                    "n2 recuperou por TCP o terceiro registo");
            waitUntil(() -> {
                try { return Files.readString(Path.of("eventos-p0-n2.txt"))
                        .contains(";RECUPERACAO;p0;"); }
                catch (Exception e) { return false; }
            }, "evento RECUPERACAO não foi registado");
            check(Files.readString(Path.of("eventos-p0-n2.txt"))
                            .contains(";RECUPERACAO;p0;"),
                    "recuperação TCP registada");
            System.out.println("PartitionNodeBlock2Test: " + passed + " verificações passaram");
        } finally {
            for (PartitionNode node : nodes) {
                if (node != null) node.close();
            }
        }
    }
    private static RaftState raftUnchecked(PartitionNode node) {
        try { return raft(node); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
}
