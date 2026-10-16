package rep01;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class PubSubReplica {
    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";
        String api = "http://127.0.0.1:" + port + "/api/v0";

        RecordFile file = new RecordFile("replica-" + id + ".txt");

        AtomicLong last = new AtomicLong();
        AtomicLong escritos = new AtomicLong(0);
        AtomicLong recebidos = new AtomicLong(0);
        AtomicLong firstReceived = new AtomicLong(0);
        AtomicLong lastReceived = new AtomicLong(0);
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        // Monitor único para proteger o estado da réplica
        final Object monitor = new Object();


        try {
            last.set(file.lastSeq());
        } catch (IOException e) {
            System.out.println("Erro ao ler último seq: " + e.getMessage());
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (monitor) {
                System.out.println("\n===== RELATÓRIO FINAL PUBSUB =====");
                System.out.println("Recebidos: " + recebidos.get());
                System.out.println("Escritos: " + escritos.get());
                System.out.println("Pendentes: " + pending.size());

                if (recebidos.get() > 0) {
                    long primeiro = firstReceived.get();
                    long ultimo = lastReceived.get();

                    System.out.println("FirstReceived: " + primeiro);
                    System.out.println("LastReceived: " + ultimo);
                    System.out.println("Intervalo de receção: "
                            + (ultimo - primeiro) + " ms");
                }

                System.out.println("================================");
            }
        }));

        IpfsPubSub pubsub = new IpfsPubSub(api);

        System.out.println("Réplica " + id + " subscreve rep01-temperaturas no nó " + api);

        try {
            pubsub.subscribe("rep01-temperaturas", message -> {

                synchronized (monitor) {

                    // Contabilizar todas as mensagens recebidas
                    long now = System.currentTimeMillis();

                    recebidos.incrementAndGet();
                    lastReceived.set(now);

                    if (firstReceived.get() == 0) {
                        firstReceived.set(now);
                    }

                    try {
                        SensorRecord r = SensorRecord.fromLine(message);
                        long seq = r.getSeq();

                        if (seq <= last.get() || pending.containsKey(seq)) {
                            System.out.println("DUPLICADO: " + seq);
                            return;
                        }

                        if (seq == last.get() + 1) {
                            escritos.addAndGet(
                                    appendAndAdvance(file, last, pending, r)
                            );

                            System.out.println("Total escritos: " + escritos.get());

                        } else {
                            pending.put(seq, r);
                            System.out.println(
                                    "EM ESPERA: " + seq +
                                            " (falta o registo " + (last.get() + 1) + ")"
                            );
                        }

                    } catch (IllegalArgumentException e) {
                        System.out.println("REJEITADO: " + message);
                    } catch (IOException e) {
                        System.out.println("IO ao escrever ficheiro: " + e.getMessage());
                    }
                }
            });

        } catch (IOException | InterruptedException e) {
            System.out.println("Erro de subscrição: " + e.getMessage());
        }
    }

    private static int appendAndAdvance(
            RecordFile file,
            AtomicLong last,
            TreeMap<Long, SensorRecord> pending,
            SensorRecord r) throws IOException {

        file.append(r);
        last.set(r.getSeq());

        int gravados = 1;

        System.out.println("Aplicado: " + r.toLine());

        while (pending.containsKey(last.get() + 1)) {
            SensorRecord next = pending.get(last.get() + 1);

            file.append(next);
            pending.remove(next.getSeq());
            last.set(next.getSeq());

            gravados++;

            System.out.println("Aplicado retido: " + next.toLine());
        }

        return gravados;
    }
}