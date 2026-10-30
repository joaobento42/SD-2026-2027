package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

public final class PartitionReplica {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições> [portoApi]");
            return;
        }
        int id = Integer.parseInt(args[0]);
        String[] lista = args[1].split(",");
        String portoApi = args.length > 2 ? args[2] : "5001";
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";

        double perda = Double.parseDouble(System.getProperty("perda", "0.0"));
        if (Double.isNaN(perda) || perda < 0.0 || perda > 1.0) {
            throw new IllegalArgumentException("O valor da propriedade perda deve estar entre 0.0 e 1.0");
        }
        long descartar = Long.parseLong(System.getProperty("descartar", "-1"));

        IpfsPubSub ps = new IpfsPubSub(api);
        DiarioEscritas diario = new DiarioEscritas("replica-" + id + "-escritas.txt");

        for (String s : lista) {
            int k = Integer.parseInt(s.trim());
            Particao p = new Particao(id, k, ps, diario, perda, descartar);
            new Thread(p::executar, "replica-" + id + "-p" + k).start();
        }
        System.out.println("Réplica " + id + ": partições " + args[1] + ", API " + api
                + (perda > 0 ? ", perda=" + perda : "") + (descartar >= 0 ? ", descartar=" + descartar : ""));
    }

    private static final class Particao {
        private final int k;
        private final IpfsPubSub ps;
        private final DiarioEscritas diario;
        private final RecordFile file;
        private final double perda;
        private final long descartar;

        private long ultimoSeq;
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        private String enderecoLider;
        private int portoLider;
        private boolean jaDescartou;

        Particao(int id, int k, IpfsPubSub ps, DiarioEscritas diario, double perda, long descartar) {
            this.k = k;
            this.ps = ps;
            this.diario = diario;
            this.file = new RecordFile("replica-" + id + "-p" + k + ".txt");
            this.perda = perda;
            this.descartar = descartar;
        }

        void executar() {
            try {
                ultimoSeq = file.lastSeq();
            } catch (Exception e) {
                System.out.println("Erro a ler " + file.name() + ": " + e.getMessage());
                return;
            }
            ps.subscribeForever(Wire.topicData(k), this::aoReceber);
        }

        private void aoReceber(String msg) {
            SensorRecord r;
            try {
                String[] partes = Wire.regParts(msg);
                enderecoLider = partes[0];
                portoLider = Integer.parseInt(partes[1]);
                r = SensorRecord.fromLine(partes[2]);
            } catch (RuntimeException e) {
                System.out.println("REJEITADO: " + msg);
                return;
            }

            if (Math.random() < perda) {
                System.out.println("DESCARTADO (simulação): p" + k + "#" + r.getSeq());
                return;
            }
            if (r.getSeq() == descartar && !jaDescartou) {
                jaDescartou = true;
                System.out.println("DESCARTADO (simulação): p" + k + "#" + r.getSeq());
                return;
            }

            try {
                aplicar(r, "pubsub");
                if (retidos.size() > Wire.LIMITE_RETIDOS) {
                    recuperar();
                }
            } catch (IOException e) {
                System.out.println("Erro a escrever na partição " + k + ": " + e.getMessage());
            }
        }

        private void aplicar(SensorRecord r, String via) throws IOException {
            long s = r.getSeq();
            if (s <= ultimoSeq || retidos.containsKey(s)) {
                if (!"tcp".equals(via)) {
                    System.out.println("DUPLICADO: p" + k + "#" + s);
                }
            } else if (s == ultimoSeq + 1) {
                escrever(r, via);
                escreverRetidos();
            } else {
                if (!"tcp".equals(via)) {
                    System.out.println("EM ESPERA: p" + k + "#" + s);
                }
                retidos.put(s, r);
            }
        }

        private void escrever(SensorRecord r, String via) throws IOException {
            file.append(r);
            ultimoSeq = r.getSeq();
            diario.registar(k, via, r);
        }

        private void escreverRetidos() throws IOException {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimoSeq + 1) {
                Map.Entry<Long, SensorRecord> e = retidos.pollFirstEntry();
                escrever(e.getValue(), "retido");
            }
        }

        private void recuperar() throws IOException {
            long de = ultimoSeq + 1;
            long ate = retidos.firstKey() - 1;
            if (de > ate) {
                return;
            }
            System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + enderecoLider + ":" + portoLider);

            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(enderecoLider, portoLider), Wire.T_TCP_MS);
                s.setSoTimeout(Wire.T_TCP_MS);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);

                out.println("PEDIDO " + de + " " + ate);
                String linha;
                while ((linha = in.readLine()) != null) {
                    if ("FIM".equals(linha)) {
                        if (ultimoSeq < ate) {
                            System.out.println("RECUPERAÇÃO FALHOU: resposta incompleta, faltam registos até " + ate);
                        }
                        return;
                    }
                    if (linha.startsWith("ERRO")) {
                        System.out.println("RECUPERAÇÃO FALHOU: " + linha);
                        return;
                    }
                    SensorRecord r = SensorRecord.fromLine(linha);
                    if (r.getSeq() == ultimoSeq + 1) {
                        System.out.println("RECUPERADO: " + r.getSeq());
                    }
                    aplicar(r, "tcp");
                }
                System.out.println("RECUPERAÇÃO FALHOU: ligação encerrada sem FIM");
            } catch (IOException | RuntimeException e) {
                System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
            } finally {
                escreverRetidos();
            }
        }
    }

    private static final class DiarioEscritas {
        private final Path path;

        DiarioEscritas(String fileName) {
            this.path = Paths.get(fileName);
        }

        synchronized void registar(int k, String via, SensorRecord r) throws IOException {
            String linha = System.currentTimeMillis() + ";p" + k + ";" + via + ";" + r.toLine() + System.lineSeparator();
            Files.writeString(path, linha, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }
}
