package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.net.SocketTimeoutException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        AtomicLong last = new AtomicLong();
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        try {
            last.set(file.lastSeq());
        } catch (IOException e) {
            System.out.println("Erro ao ler último seq: " + e.getMessage());
            return;
        }

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            socket.setSoTimeout(1000);
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);

            byte[] buffer = new byte[1000];

            long recebidos = 0;
            long escritos = 0;
            long firstReceived = 0;
            long lastReceived = 0;

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);

                try {
                    socket.receive(p);

                } catch (SocketTimeoutException e) {
                    if (recebidos > 0) {
                        System.out.println("\n===== RELATÓRIO =====");
                        System.out.println("Recebidos: " + recebidos);
                        System.out.println("Escritos: " + escritos);
                        System.out.println("Pendentes: " + pending.size());
                        System.out.println("FirstReceived: " + firstReceived);
                        System.out.println("LastReceived: " + lastReceived);
                        System.out.println("=====================");

                        // Reiniciar apenas a medição
                        recebidos = 0;
                        escritos = 0;
                        firstReceived = 0;
                        lastReceived = 0;
                    }

                    continue;
                }

// Atualizar contadores após receber datagrama
                recebidos++;

                long now = System.currentTimeMillis();

                if (firstReceived == 0) {
                    firstReceived = now;
                }

                lastReceived = now;

                String line = new String(
                        p.getData(),
                        0,
                        p.getLength(),
                        StandardCharsets.UTF_8
                );

                try {
                    SensorRecord r = SensorRecord.fromLine(line);
                    long seq = r.getSeq();

                    if (seq <= last.get() || pending.containsKey(seq)) {
                        System.out.println("DUPLICADO: " + seq);
                        continue;
                    }

                    if (seq == last.get() + 1) {
                        escritos += appendAndAdvance(file, last, pending, r);
                    }
                    else {
                        pending.put(seq, r);
                        System.out.println(
                                "EM ESPERA: " + seq +
                                        " (falta o registo " + (last.get() + 1) + ")"
                        );
                    }

                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                } catch (IOException e) {
                    System.out.println("IO ao escrever ficheiro: " + e.getMessage());
                }
            }

        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
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