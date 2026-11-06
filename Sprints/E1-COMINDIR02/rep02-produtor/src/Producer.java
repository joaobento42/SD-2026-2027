package rep02;

import java.util.Random;
import java.util.Scanner;

public class Producer {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        Scanner scanner = new Scanner(System.in);
        Random rand = new Random();

        System.out.println("Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s>");

        while (scanner.hasNextLine()) {
            String linha = scanner.nextLine().trim();
            if (linha.isEmpty()) continue;

            String[] partes = linha.split("\\s+");

            try {
                if ("rajada".equals(partes[0]) || "rajada-quente".equals(partes[0])) {
                    if (partes.length != 3) {
                        System.out.println("Uso: " + partes[0] + " <n> <s>");
                        continue;
                    }
                    int qtd = Integer.parseInt(partes[1]);
                    int s = Integer.parseInt(partes[2]);
                    boolean quente = "rajada-quente".equals(partes[0]);

                    if (qtd <= 0 || s <= 0) {
                        System.out.println("Erro: a quantidade (n) e o numero de sensores (s) devem ser > 0.");
                        continue;
                    }

                    if (quente && s < 2) {
                        System.out.println("Erro: para rajada-quente, s deve ser no minimo 2.");
                        continue;
                    }

                    long inicio = 0;
                    long fim = 0;
                    int[] contagem = new int[n];

                    for (int i = 0; i < qtd; i++) {
                        String sensor;
                        if (quente) {
                            if (rand.nextDouble() < 0.8) {
                                sensor = "S01";
                            } else {
                                int num = 2 + rand.nextInt(s - 1);
                                sensor = String.format("S%02d", num);
                            }
                        } else {
                            int num = 1 + rand.nextInt(s);
                            sensor = String.format("S%02d", num);
                        }

                        // Arredondamento a 1 casa decimal permite a inclusao exata de 30.0
                        double temp = Math.round((15.0 + rand.nextDouble() * 15.0) * 10.0) / 10.0;
                        int p = partitioner.of(sensor);

                        String msg = Wire.in(sensor, temp);

                        long instante = System.currentTimeMillis();
                        if (i == 0) inicio = instante;
                        if (i == qtd - 1) fim = instante;

                        ps.publish(Wire.topicIn(p), msg);

                        System.out.println(msg + " -> p" + p);
                        contagem[p]++;
                    }

                    System.out.println("Rajada enviada. Inicio: " + inicio + " Fim: " + fim);
                    for (int i = 0; i < n; i++) {
                        System.out.println("p" + i + ": " + contagem[i] + " leituras");
                    }

                } else if (partes.length == 2) {
                    String sensor = partes[0];
                    double temp = Double.parseDouble(partes[1]);
                    int p = partitioner.of(sensor);

                    String msg = Wire.in(sensor, temp);
                    ps.publish(Wire.topicIn(p), msg);
                    System.out.println("Publicado na p" + p + ": " + msg);
                } else {
                    System.out.println("Comando invalido.");
                }
            } catch (Exception e) {
                System.out.println("Erro ao processar comando: " + e.getMessage());
            }
        }
        scanner.close();
    }
}