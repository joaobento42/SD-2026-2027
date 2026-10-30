package rep03;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public final class Producer {
    private final int n;
    private final Partitioner partitioner;
    private final LeaderClient lideres;
    private final Random rand = new Random();

    private Producer(int n, String enderecosLideres) {
        this.n = n;
        this.partitioner = new Partitioner(n);
        this.lideres = new LeaderClient(n, enderecosLideres);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [endereçosLíderes]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Producer p = new Producer(n, args.length > 1 ? args[1] : null);
        p.executar();
    }

    private void executar() {
        System.out.println("Produtor com N = " + n + "; líderes em " + descreverLideres());
        System.out.println("Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s> | sair");

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String linha = scanner.nextLine().trim();
            if (linha.isEmpty()) {
                continue;
            }
            String[] partes = linha.split("\\s+");
            try {
                if ("sair".equals(partes[0])) {
                    break;
                } else if ("rajada".equals(partes[0]) || "rajada-quente".equals(partes[0])) {
                    if (partes.length != 3) {
                        System.out.println("Uso: " + partes[0] + " <n> <s>");
                        continue;
                    }
                    rajada(Integer.parseInt(partes[1]), Integer.parseInt(partes[2]), "rajada-quente".equals(partes[0]));
                } else if (partes.length == 2) {
                    enviar(partes[0], Double.parseDouble(partes[1].replace(',', '.')));
                } else {
                    System.out.println("Comando inválido.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Valor numérico inválido: " + e.getMessage());
            }
        }
        lideres.close();
    }

    private int enviar(String sensor, double temp) {
        int p = partitioner.of(sensor);
        String msg = Wire.in(sensor, temp, 0, new VectorClock(n));
        try {
            lideres.send(p, msg);
            System.out.println(msg + " → p" + p);
            return p;
        } catch (IOException e) {
            System.out.println("PERDIDA: " + msg + " → p" + p + " (" + e.getMessage() + ")");
            return -1;
        }
    }

    private void rajada(int qtd, int s, boolean quente) {
        if (qtd <= 0 || s <= 0) {
            System.out.println("Erro: n e s devem ser > 0.");
            return;
        }
        if (quente && s < 2) {
            System.out.println("Erro: para rajada-quente, s deve ser no mínimo 2.");
            return;
        }

        int[] porParticao = new int[n];
        int perdidas = 0;
        long inicio = 0;
        long fim = 0;

        for (int i = 0; i < qtd; i++) {
            String sensor;
            if (quente) {
                sensor = rand.nextDouble() < 0.8 ? "S01" : String.format("S%02d", 2 + rand.nextInt(s - 1));
            } else {
                sensor = String.format("S%02d", 1 + rand.nextInt(s));
            }
            double temp = Math.round((15.0 + rand.nextDouble() * 15.0) * 10.0) / 10.0;

            long instante = System.currentTimeMillis();
            if (i == 0) inicio = instante;
            if (i == qtd - 1) fim = instante;

            int p = enviar(sensor, temp);
            if (p >= 0) {
                porParticao[p]++;
            } else {
                perdidas++;
            }
        }

        System.out.println("Rajada enviada. Início: " + inicio + " Fim: " + fim + " (" + (fim - inicio) + " ms)");
        for (int k = 0; k < n; k++) {
            System.out.println("p" + k + ": " + porParticao[k] + " leituras");
        }
        if (perdidas > 0) {
            System.out.println("Perdidas (líder inacessível): " + perdidas);
        }
    }

    private String descreverLideres() {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < n; k++) {
            if (k > 0) sb.append(", ");
            sb.append("p").append(k).append("=").append(lideres.endereco(k));
        }
        return sb.toString();
    }
}
