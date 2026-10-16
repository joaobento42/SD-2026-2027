package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair' para terminar");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                if (line.equalsIgnoreCase("rajada")) {
                    System.out.println("Use: rajada <n>");
                    continue;
                }
                if (line.toLowerCase().startsWith("rajada ")) {
                    int n;
                    try {
                        n = Integer.parseInt(line.substring("rajada ".length()).trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Número inválido para rajada.");
                        continue;
                    }
                    long firstSent = System.currentTimeMillis();
                    long lastSent = firstSent;
                    Random rnd = new Random();
                    for (int i = 0; i < n; i++) {
                        double temp = 15 + 15 * rnd.nextDouble();
                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                        file.append(r);
                        byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                        socket.send(new DatagramPacket(m, m.length, group, PORT));
                        lastSent = System.currentTimeMillis();
                    }
                    System.out.println("Rajada concluída: primeiro=" + firstSent + "ms, último=" + lastSent + "ms, registos=" + n);
                    continue;
                }
                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT));
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
