package raft04;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Testes locais sem daemon Kubo: executar numa pasta temporária vazia. */
public final class ProducerBlock4Test {
    private static int checks;

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
        checks++;
    }

    private static void invalid(Runnable action, String message) {
        boolean failed = false;
        try { action.run(); } catch (IllegalArgumentException ex) { failed = true; }
        check(failed, message);
    }

    private static HeartbeatMonitor[] monitors(List<String> events) {
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[3];
        for (int k = 0; k < monitors.length; k++) {
            monitors[k] = new HeartbeatMonitor(k, 2_000, 3, events::add);
        }
        return monitors;
    }

    private static void forceSuspect(HeartbeatMonitor monitor) throws Exception {
        Field f = HeartbeatMonitor.class.getDeclaredField("lastNanos");
        f.setAccessible(true);
        f.setLong(monitor, System.nanoTime() - 7_000_000_000L);
        monitor.check();
    }

    private static final class FakePubSub extends IpfsPubSub {
        private final List<String> topics = new ArrayList<>();
        private final List<String> messages = new ArrayList<>();
        private boolean fail;
        FakePubSub() { super("http://127.0.0.1:5001/api/v0"); }
        @Override
        public void publish(String topic, String message) throws IOException {
            if (fail) throw new IOException("falha simulada");
            topics.add(topic);
            messages.add(message);
        }
    }

    private static long publish(Producer producer, String sensor, double temp, FakePubSub pubSub,
                                Partitioner partitioner, HeartbeatMonitor[] monitors)
            throws Exception {
        Method m = Producer.class.getDeclaredMethod("publishReading", String.class, double.class,
                IpfsPubSub.class, Partitioner.class, HeartbeatMonitor[].class);
        m.setAccessible(true);
        try {
            return (long) m.invoke(producer, sensor, temp, pubSub, partitioner, monitors);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof IOException ex) throw ex;
            if (e.getCause() instanceof RuntimeException ex) throw ex;
            throw e;
        }
    }

    private static void deliver(Producer producer, int partition, HeartbeatMonitor[] monitors,
                                String message) throws Exception {
        Method m = Producer.class.getDeclaredMethod("onHeartbeat", int.class, int.class,
                HeartbeatMonitor[].class, String.class);
        m.setAccessible(true);
        m.invoke(producer, partition, 3, monitors, message);
    }

    public static void main(String[] args) throws Exception {
        check(Wire.PERIODO_HB_MS == 2000, "Período padrão de 2s");
        check(Wire.VERIFICACAO_MS == 200, "Verificação de 200ms");

        List<String> events = new ArrayList<>();
        HeartbeatMonitor monitor = new HeartbeatMonitor(1, 2_000, 3, events::add);
        check(monitor.state() == HeartbeatMonitor.State.DESCONHECIDO, "Inicial desconhecido");
        check(monitor.term() == -1, "Termo ainda desconhecido");
        invalid(() -> monitor.onHeartbeat(-1, 1, 1), "Termo negativo rejeitado");
        invalid(() -> monitor.onHeartbeat(1, 0, 1), "Líder zero rejeitado");
        invalid(() -> monitor.onHeartbeat(1, 4, 1), "Líder >3 rejeitado");
        invalid(() -> monitor.onHeartbeat(1, 2, 0), "Hb zero rejeitado");

        monitor.onHeartbeat(1, 2, 1);
        check(monitor.state() == HeartbeatMonitor.State.ATIVO, "Ativo após heartbeat");
        check(monitor.term() == 1, "Termo inicial");
        check(monitor.leader() == 2, "Líder inicial");
        check(monitor.heartbeat() == 1, "Contador inicial");
        check(events.size() == 1 && events.get(0).startsWith("NOVO_LIDER;p1;"), "Evento novo líder inicial");
        monitor.onHeartbeat(1, 2, 1);
        check(events.size() == 1, "Duplicado sem evento");
        monitor.onHeartbeat(0, 1, 5);
        check(monitor.term() == 1, "Termo antigo ignorado");
        monitor.onHeartbeat(1, 3, 8);
        check(monitor.leader() == 2, "Segundo líder mesmo termo ignorado");
        monitor.onHeartbeat(1, 2, 4);
        check(events.stream().anyMatch(x -> x.startsWith("HB_PERDIDO;p1;")
                && x.contains("esperado=2") && x.contains("recebido=4")), "Salto de hb assinalado");
        check(monitor.heartbeat() == 4, "Salto de hb aceite");
        monitor.onHeartbeat(2, 3, 1);
        check(monitor.term() == 2 && monitor.leader() == 3 && monitor.heartbeat() == 1,
                "Termo superior substitui líder e reinicia baseline hb");
        check(events.stream().anyMatch(x -> x.contains("NOVO_LIDER;p1;termo=2;lider=n3")),
                "Novo líder assinalado em mudança de termo");
        check(events.stream().filter(x -> x.startsWith("HB_PERDIDO;")).count() == 1,
                "Reset de hb entre termos não é perda");
        forceSuspect(monitor);
        check(monitor.state() == HeartbeatMonitor.State.SUSPEITO, "Timeout marca suspeito");
        int countBefore = events.size();
        monitor.check();
        check(events.size() == countBefore, "Suspeita não repete evento");
        monitor.onHeartbeat(1, 2, 999);
        check(monitor.state() == HeartbeatMonitor.State.SUSPEITO, "Heartbeat antigo não recupera suspeita");
        monitor.onHeartbeat(2, 3, 2);
        check(monitor.state() == HeartbeatMonitor.State.ATIVO, "Heartbeat válido recupera monitor");
        check(events.stream().anyMatch(x -> x.startsWith("RETOMA;p1;")), "Evento retoma");

        Path producerFile = Path.of("produtor.txt");
        Path monitorFile = Path.of("monitor-produtor.txt");
        check(!Files.exists(producerFile), "Pasta de teste sem dados antigos de produtor");
        check(!Files.exists(monitorFile), "Pasta de teste sem monitor antigo");
        Producer producer = new Producer();
        Partitioner partitioner = new Partitioner(3);
        List<String> allEvents = new ArrayList<>();
        HeartbeatMonitor[] monitors = monitors(allEvents);
        FakePubSub fake = new FakePubSub();
        long t1 = publish(producer, "S01", 21.5, fake, partitioner, monitors);
        int p = partitioner.of("S01");
        check(fake.messages.size() == 1, "Primeiro IN publicado");
        check(fake.topics.get(0).equals(Wire.topicIn(p)), "Tópico IN correto");
        check(fake.messages.get(0).startsWith("IN;S01;21.5;0;"), "Payload IN inalterado");
        List<String> lines = Files.readAllLines(producerFile, StandardCharsets.UTF_8);
        check(lines.size() == 1, "Primeiro registo em produtor.txt");
        check(lines.get(0).equals(t1 + ";p" + p + ";S01;21.5"), "Formato produtor.txt");

        forceSuspect(monitors[p]);
        publish(producer, "S01", 23.0, fake, partitioner, monitors);
        check(fake.messages.size() == 2, "Mesmo suspeito ainda publica");
        lines = Files.readAllLines(producerFile, StandardCharsets.UTF_8);
        check(lines.size() == 2, "Mesmo suspeito ainda regista IN");
        check(Files.readString(monitorFile).contains(";AVISO;p" + p + ";leitura=S01"),
                "Aviso escrito no monitor-produtor.txt");

        fake.fail = true;
        boolean failed = false;
        try { publish(producer, "S01", 24.0, fake, partitioner, monitors); }
        catch (IOException ex) { failed = true; }
        check(failed, "Erro de publicação reportado");
        check(Files.readAllLines(producerFile).size() == 2,
                "Publicação falhada não é contada como IN enviado");

        HeartbeatMonitor[] decoderMonitors = monitors(new ArrayList<>());
        Batch valid = new Batch(4, 2, "127.0.0.1", 10001, 1, 0, 0, List.of());
        deliver(producer, 0, decoderMonitors, valid.encode());
        check(decoderMonitors[0].term() == 4, "Producer interpreta termo do Batch");
        check(decoderMonitors[0].leader() == 2, "Producer interpreta líder do Batch");
        check(decoderMonitors[0].heartbeat() == 1, "Producer interpreta hb do Batch");
        deliver(producer, 0, decoderMonitors, "LOTE;3;1;127.0.0.1;10000;99;0;0;0");
        check(decoderMonitors[0].term() == 4, "Producer ignora lote antigo");
        deliver(producer, 0, decoderMonitors, "LOTE;malformado");
        check(decoderMonitors[0].heartbeat() == 1, "Producer rejeita cabeçalho inválido");
        deliver(producer, 0, decoderMonitors, "LOTE;5;3;127.0.0.1;10002;1;1;5;1");
        check(decoderMonitors[0].term() == 4, "Producer rejeita lote sem linha de registo");

        System.out.println("ProducerBlock4Test: " + checks + " verificações OK");
    }
}
