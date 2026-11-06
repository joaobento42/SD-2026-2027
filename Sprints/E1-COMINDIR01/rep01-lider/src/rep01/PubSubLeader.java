package rep01;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class PubSubLeader {
    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubsub = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider.txt");
        long seq = 0;
        try {
            seq = file.lastSeq();
        } catch (IOException e) {
            System.out.println("Não foi possível ler o último seq: " + e.getMessage());
         return;
        }
        try (Scanner sc = new Scanner(System.in)) {
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
                        try {
                            file.append(r);
                            pubsub.publish("rep01-temperaturas", r.toLine());
                        } catch (IOException | InterruptedException e) {
                            System.out.println("Erro ao publicar: " + e.getMessage());
                            return;
                        }
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
                try {
                    file.append(r);
                    pubsub.publish("rep01-temperaturas", r.toLine());
                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro ao publicar: " + e.getMessage());
                    return;
                }
                System.out.println("Registado e publicado: " + r.toLine());
            }
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}
