package raft04;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Regressões do Bloco 3: só correr numa pasta de trabalho vazia. */
public final class PartitionNodeBlock3RegressionTest {
    private static int passed;
    private static void ok(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
        passed++;
    }
    private static final class Bus {
        final Map<String, CopyOnWriteArrayList<Consumer<String>>> channels = new ConcurrentHashMap<>();
        void add(String topic, Consumer<String> cb) {
            channels.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(cb);
        }
        void remove(String topic, Consumer<String> cb) {
            channels.getOrDefault(topic, new CopyOnWriteArrayList<>()).remove(cb);
        }
        int count(String topic) {return channels.getOrDefault(topic, new CopyOnWriteArrayList<>()).size();}
        void publish(String topic, String payload) {
            for (Consumer<String> cb : channels.getOrDefault(topic,
                    new CopyOnWriteArrayList<>())) cb.accept(payload);
        }
    }
    private static final class MockPubSub extends IpfsPubSub {
        final Bus bus;
        MockPubSub(Bus bus) { super("http://127.0.0.1:5001/api/v0"); this.bus=bus; }
        @Override public void publish(String topic, String msg) {bus.publish(topic, msg);}
        @Override public void subscribe(String topic, Consumer<String> cb)
                throws InterruptedException {
            bus.add(topic, cb);
            try { while (true) Thread.sleep(60); }
            finally { bus.remove(topic, cb); }
        }
    }
    private static SensorRecord record(int seq, long term, String sensor) {
        return new SensorRecord(seq, term, sensor, 20 + seq,
                Long.toString(101000L + seq), seq, VectorClock.parse(seq + ",0,0", 3));
    }
    private static void until(java.util.function.BooleanSupplier condition, String message)
            throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(6);
        while (!condition.getAsBoolean()) {
            if(System.nanoTime()>=end) throw new AssertionError(message);
            Thread.sleep(20);
        }
    }
    private static void checkIdentity() throws Exception {
        Method same = PartitionNode.class.getDeclaredMethod("sameRecord",
                SensorRecord.class, SensorRecord.class);
        same.setAccessible(true);
        ok((boolean) same.invoke(null, record(1, 2, "A"), record(1, 2, "B")),
                "A identidade é seq+term, não o payload");
        ok(!(boolean) same.invoke(null, record(1, 2, "A"), record(1, 3, "A")),
                "Termos diferentes não coincidem");
    }
    private static boolean recoveryIdle(PartitionNode node) {
        try {
            Field lockField = PartitionNode.class.getDeclaredField("nodeLock");
            lockField.setAccessible(true);
            Field runningField = PartitionNode.class.getDeclaredField("recoveryScheduled");
            runningField.setAccessible(true);
            Object lock = lockField.get(node);
            synchronized (lock) { return !(boolean) runningField.get(node); }
        } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static void staleRecovery() throws Exception {
        Path state = Path.of("estado-p0-n1.txt");
        Files.writeString(state, "2;0");
        List<SensorRecord> original = List.of(record(1,1,"A"),record(2,1,"B"),record(3,1,"C"));
        new RecordFile("no-p0-n1.txt").replaceAll(original);
        String originalData = Files.readString(Path.of("no-p0-n1.txt"));
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch releaseResponse = new CountDownLatch(1);
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        Bus bus = new Bus();
        try(ServerSocket fakeLeader = new ServerSocket(Wire.nodePort(0, 2));
            PartitionNode follower = new PartitionNode(0,1,3,"127.0.0.1",new MockPubSub(bus))) {
            Thread server = new Thread(() -> {
                try(Socket socket = fakeLeader.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(
                            socket.getInputStream(),StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(new OutputStreamWriter(
                            socket.getOutputStream(),StandardCharsets.UTF_8),true)) {
                    String request = in.readLine();
                    if (!"PEDIDO 1 2".equals(request))
                        throw new IllegalStateException("Pedido inesperado: " + request);
                    requestStarted.countDown();
                    if (!releaseResponse.await(4, TimeUnit.SECONDS))
                        throw new IllegalStateException("Timeout no teste TCP");
                    out.println(original.get(0).toLine());
                    out.println(original.get(1).toLine());
                    out.println("FIM");
                } catch (Throwable e) {serverFailure.set(e); requestStarted.countDown();}
            }, "tcp-held-response");
            server.start();
            try {
                follower.start();
                until(() -> bus.count(Wire.topicData(0)) == 1, "Faltou subscrição");
                // Primeiro lote força recuperar e truncar seq 3, mas a resposta fica bloqueada.
                bus.publish(Wire.topicData(0), new Batch(4,2,"127.0.0.1",10001,
                        1,2,1,List.of()).encode());
                ok(requestStarted.await(3,TimeUnit.SECONDS), "Iniciou recuperação TCP antiga");
                ok(serverFailure.get()==null, "Pedido TCP recebido sem erro");
                // Outro heartbeat sobre o mesmo estado NÃO deve cancelar recovery.
                bus.publish(Wire.topicData(0), new Batch(4,2,"127.0.0.1",10001,
                        2,2,1,List.of()).encode());
                // LOTE posterior altera o alvo: seq 3 afinal é válido, não deve truncar.
                bus.publish(Wire.topicData(0), new Batch(4,2,"127.0.0.1",10001,
                        3,3,1,List.of()).encode());
                releaseResponse.countDown();
                server.join(4000);
                ok(!server.isAlive() && serverFailure.get()==null,
                        "Resposta TCP antiga terminada corretamente");
                until(() -> recoveryIdle(follower), "Worker de recuperação não terminou");
                ok(originalData.equals(Files.readString(Path.of("no-p0-n1.txt"))),
                        "Recuperação antiga não trunca registo validado por lote novo");
                // Um LOTE obsoleto do mesmo termo/líder também não pode repor o alvo.
                bus.publish(Wire.topicData(0), new Batch(4,2,"127.0.0.1",10001,
                        1,2,1,List.of()).encode());
                ok(originalData.equals(Files.readString(Path.of("no-p0-n1.txt"))),
                        "Heartbeat antigo ignorado após o mais recente");
            } finally {
                releaseResponse.countDown();
                server.join(4000);
            }
        }
    }
    private static void candidacyTermEvent() throws Exception {
        Files.writeString(Path.of("estado-p1-n1.txt"), "2;0");
        Bus bus = new Bus();
        try(PartitionNode node = new PartitionNode(1,1,3,"127.0.0.1",new MockPubSub(bus))) {
            node.start();
            Field raftField = PartitionNode.class.getDeclaredField("raft");
            raftField.setAccessible(true);
            RaftState raft = (RaftState)raftField.get(node);
            Field deadline = RaftState.class.getDeclaredField("deadline");
            deadline.setAccessible(true);
            synchronized(raft) { deadline.setLong(raft,System.nanoTime()-1); }
            Method check = PartitionNode.class.getDeclaredMethod("checkElection");
            check.setAccessible(true);
            check.invoke(node);
            String events = Files.readString(Path.of("eventos-p1-n1.txt"));
            ok(events.contains(";TERMO;p1;de=2;para=3;origem=CANDIDATURA"),
                    "A candidatura regista TERMO");
            ok(events.contains(";CANDIDATO;p1;termo=3"),
                    "A candidatura continua a registar CANDIDATO");
        }
    }
    public static void main(String[] args) throws Exception {
        ok(!Files.exists(Path.of("no-p0-n1.txt")), "Executar em diretório vazio");
        checkIdentity();
        staleRecovery();
        candidacyTermEvent();
        System.out.println("PartitionNodeBlock3RegressionTest: " + passed + " verificações passaram");
    }
}
