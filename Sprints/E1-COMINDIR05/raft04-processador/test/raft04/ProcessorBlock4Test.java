package raft04;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Teste local sem Kubo. Executar numa pasta temporária vazia. */
public final class ProcessorBlock4Test {
    private static int checks = 0;

    private static void check(boolean condition, String description) {
        if (!condition) throw new AssertionError(description);
        checks++;
    }

    private static final class FakePubSub extends IpfsPubSub {
        final List<String> topics = new ArrayList<>();
        final List<String> messages = new ArrayList<>();
        boolean fail;
        FakePubSub() { super("http://127.0.0.1:5001/api/v0"); }
        @Override
        public void publish(String topic, String message) throws IOException {
            if (fail) throw new IOException("Falha simulada");
            topics.add(topic);
            messages.add(message);
        }
    }

    private static void deliver(AlarmProcessor processor, int partition, String payload)
            throws Exception {
        Method method = AlarmProcessor.class.getDeclaredMethod("onMessage", int.class, String.class);
        method.setAccessible(true);
        method.invoke(processor, partition, payload);
    }

    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    @SuppressWarnings("unchecked")
    private static List<HeartbeatMonitor> monitors(AlarmProcessor processor) throws Exception {
        return (List<HeartbeatMonitor>) field(processor, "monitors");
    }

    @SuppressWarnings("unchecked")
    private static Set<String> seen(AlarmProcessor processor) throws Exception {
        return new HashSet<>((Set<String>) field(processor, "seen"));
    }

    private static long lamport(AlarmProcessor processor) throws Exception {
        return (long) field(processor, "lp");
    }

    private static String vector(AlarmProcessor processor) throws Exception {
        return field(processor, "vp").toString();
    }

    private static SensorRecord record(int partition, long seq, long term, double temperature,
                                       long L) {
        long[] values = {0, 0, 0};
        values[partition] = seq;
        return new SensorRecord(seq, term, "S01", temperature, "123456789", L,
                VectorClock.parse(values[0] + "," + values[1] + "," + values[2], 3));
    }

    private static Batch batch(int partition, long term, int leader, long hb,
                               SensorRecord... records) {
        long seq = 0, lastTerm = 0;
        for (SensorRecord r : records) {
            if (r.getSeq() > seq) {
                seq = r.getSeq();
                lastTerm = r.getTerm();
            }
        }
        return new Batch(term, leader, "127.0.0.1", 10000 + partition * 3 + leader - 1,
                hb, seq, lastTerm, List.of(records));
    }

    private static void forceSuspect(HeartbeatMonitor monitor) throws Exception {
        Field last = HeartbeatMonitor.class.getDeclaredField("lastNanos");
        last.setAccessible(true);
        last.setLong(monitor, System.nanoTime() - 10_000_000_000L);
        monitor.check();
    }

    public static void main(String[] args) throws Exception {
        check(Wire.PERIODO_HB_MS == 2000L, "Período padrão de 2 segundos");
        check(Wire.VERIFICACAO_MS == 200L, "Verificação a cada 200 ms");

        List<String> events = new ArrayList<>();
        HeartbeatMonitor m = new HeartbeatMonitor(0, 2000L, 3, events::add);
        check(m.state() == HeartbeatMonitor.State.DESCONHECIDO, "Estado desconhecido");
        check(m.term() == -1, "Termo desconhecido");
        check(m.leader() == 0, "Líder desconhecido");
        check(m.onHeartbeat(4, 2, 1), "Primeiro heartbeat aceite");
        check(m.term() == 4 && m.leader() == 2 && m.heartbeat() == 1, "Liderança conhecida");
        check(events.stream().anyMatch(e -> e.startsWith("NOVO_LIDER;p0;termo=4;lider=n2")),
                "Novo líder registado");
        check(!m.onHeartbeat(3, 1, 999), "Termo antigo rejeitado");
        check(m.term() == 4 && m.heartbeat() == 1, "Termo antigo não altera contador");
        check(!m.onHeartbeat(4, 3, 7), "Segundo líder no mesmo termo rejeitado");
        check(m.leader() == 2, "Líder mantido");
        check(!m.onHeartbeat(4, 2, 1), "Heartbeat duplicado rejeitado");
        check(m.onHeartbeat(4, 2, 4), "Salto de heartbeat aceite");
        check(events.stream().anyMatch(e -> e.startsWith("HB_PERDIDO;p0;")),
                "Salto de heartbeat registado");
        check(m.onHeartbeat(5, 3, 1), "Novo termo troca líder");
        check(m.term() == 5 && m.leader() == 3, "Termo e líder atualizados");
        forceSuspect(m);
        check(m.state() == HeartbeatMonitor.State.SUSPEITO, "Timeout ativa suspeita");
        check(!m.onHeartbeat(4, 2, 100), "Heartbeat obsoleto não recupera suspeita");
        check(m.state() == HeartbeatMonitor.State.SUSPEITO, "Suspeita permanece");
        check(m.onHeartbeat(5, 3, 2), "Heartbeat válido recupera suspeita");
        check(m.state() == HeartbeatMonitor.State.ATIVO, "Partição voltou a ativa");
        check(events.stream().anyMatch(e -> e.startsWith("RETOMA;p0;")), "Retoma registada");

        Path logFile = Path.of("monitor-processador.txt");
        check(!Files.exists(logFile), "Pasta de teste sem log anterior");
        FakePubSub fake = new FakePubSub();
        AlarmProcessor proc = new AlarmProcessor(3, fake);
        deliver(proc, 0, batch(0, 4, 2, 1, record(0, 12, 3, 21.0, 20)).encode());
        check(monitors(proc).get(0).term() == 4, "Lote válido atualiza termo");
        check(monitors(proc).get(0).leader() == 2, "Lote válido atualiza líder");
        check(seen(proc).contains("0:12:3"), "Identidade inclui seq, partição e termo");
        check(seen(proc).size() == 1, "Primeiro registo deduplicado");
        check(lamport(proc) == 20, "Lamport acumulado");
        check(vector(proc).equals("12,0,0"), "Vetor acumulado");
        String initialLog = Files.readString(logFile);
        check(initialLog.contains("NOVO_LIDER;p0;termo=4;lider=n2"), "Novo líder escrito no log");
        check(initialLog.contains("RECEBIDO;p0;termo=4;lider=n2"), "Lote recebido registado");
        long oldHb = monitors(proc).get(0).heartbeat();
        int size = seen(proc).size();
        long oldL = lamport(proc);
        String oldV = vector(proc);
        String oldLog = Files.readString(logFile);
        deliver(proc, 0, batch(0, 3, 1, 999, record(0, 900, 3, 40.0, 800)).encode());
        check(monitors(proc).get(0).term() == 4, "Lote obsoleto não altera termo");
        check(monitors(proc).get(0).heartbeat() == oldHb, "Lote obsoleto não altera hb");
        check(seen(proc).size() == size, "Lote obsoleto não entra na deduplicação");
        check(lamport(proc) == oldL && vector(proc).equals(oldV),
                "Lote obsoleto não altera causalidade");
        check(fake.messages.isEmpty(), "Lote obsoleto não publica alarme");
        check(Files.readString(logFile).equals(oldLog), "Lote obsoleto não altera logs");
        deliver(proc, 0, batch(0, 4, 2, 2, record(0, 12, 3, 32.0, 700)).encode());
        check(seen(proc).size() == 1, "Duplicado exato ignorado");
        check(lamport(proc) == 20, "Duplicado exato não altera L");
        check(fake.messages.isEmpty(), "Duplicado exato não gera alarme");
        deliver(proc, 0, batch(0, 4, 3, 3, record(0, 13, 4, 44.0, 1000)).encode());
        check(seen(proc).size() == 1, "Líder conflituante ignorado");
        check(lamport(proc) == 20, "Líder conflituante não altera causalidade");
        deliver(proc, 0, batch(0, 5, 3, 1, record(0, 12, 5, 22.0, 21)).encode());
        check(monitors(proc).get(0).term() == 5, "Termo novo reconhecido");
        check(seen(proc).contains("0:12:5") && seen(proc).size() == 2,
                "Mesmo seq de outro termo é identidade diferente");
        check(lamport(proc) == 21, "Novo registo atualiza Lamport");
        deliver(proc, 1, batch(1, 5, 1, 1, record(1, 12, 5, 21.0, 23)).encode());
        check(seen(proc).contains("1:12:5") && seen(proc).size() == 3,
                "Mesmos seq e termo noutra partição não colidem");
        check(vector(proc).equals("12,12,0"), "Merge causal entre partições");
        int before = seen(proc).size();
        deliver(proc, 0, "LOTE;invalido");
        check(seen(proc).size() == before, "Lote malformado sem efeitos");
        check(monitors(proc).get(0).term() == 5, "Lote malformado não altera monitor");
        deliver(proc, 0, batch(0, 5, 3, 2, record(0, 13, 5, 32.0, 30)).encode());
        check(fake.messages.size() == 1, "Leitura acima do limiar gera um alarme");
        check(fake.messages.get(0).startsWith("IN;ALARM-S01;32.0;30;"),
                "Alarme mantém leitura e causalidade");
        check(fake.topics.get(0).startsWith("raft04-in-p"), "Alarme enviado para tópico de entrada");
        deliver(proc, 0, batch(0, 5, 3, 3, record(0, 13, 5, 32.0, 30)).encode());
        check(fake.messages.size() == 1, "Duplicado do alarme não republica");

        int target = new Partitioner(3).of("ALARM-S01");
        forceSuspect(monitors(proc).get(target));
        deliver(proc, 0, batch(0, 5, 3, 4, record(0, 14, 5, 35.0, 31)).encode());
        check(fake.messages.size() == 2, "Publica alarme mesmo se destino suspeito");
        check(Files.readString(logFile).contains("AVISO;p" + target + ";alarme=ALARM-S01"),
                "Suspeita do destino gera aviso");
        System.out.println("ProcessorBlock4Test: " + checks + " verificações OK");
    }
}
