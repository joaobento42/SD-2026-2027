package rep02;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider-p" + k + ".txt");
        int porto = Wire.PORTO_BASE + k;

        long[] estadoSeq = new long[1];
        estadoSeq[0] = file.lastSeq();

        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k), msg -> {
                    String sensor;
                    double temp;
                    try {
                        String tipo = Wire.type(msg);
                        if (!"IN".equals(tipo)) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        String corpo = Wire.body(msg);
                        String[] partes = corpo.split(";", 2);
                        if (partes.length < 2) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        sensor = partes[0];
                        temp = Double.parseDouble(partes[1]);

                        int j = partitioner.of(sensor);
                        if (j != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                            return;
                        }
                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg);
                        return;
                    }

                    try {
                        long proximoSeq;
                        SensorRecord r;
                        synchronized (file) {
                            proximoSeq = estadoSeq[0] + 1;
                            r = SensorRecord.now(proximoSeq, sensor, temp);
                            file.append(r);
                            estadoSeq[0] = proximoSeq;
                        }
                        ps.publish(Wire.topicData(k), Wire.reg(endereco, porto, r));
                    } catch (Exception e) {
                        System.out.println("Erro ao gravar ou publicar: " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.out.println("Erro na thread de entrada: " + e.getMessage());
            }
        });
        entrada.start();

        try (ServerSocket server = new ServerSocket(porto)) {
            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();

                    if (pedido == null || !pedido.startsWith("PEDIDO ")) {
                        out.println("ERRO Pedido invalido");
                        continue;
                    }

                    String[] partesPedido = pedido.split(" ");
                    if (partesPedido.length != 3) {
                        out.println("ERRO Formato incorreto");
                        continue;
                    }

                    long de;
                    long ate;
                    try {
                        de = Long.parseLong(partesPedido[1]);
                        ate = Long.parseLong(partesPedido[2]);
                    } catch (NumberFormatException e) {
                        out.println("ERRO Formato numerico incorreto");
                        continue;
                    }

                    if (de > ate) {
                        out.println("ERRO Intervalo invalido");
                        continue;
                    }

                    long ultimoEscrito;
                    synchronized (file) {
                        ultimoEscrito = estadoSeq[0];
                    }

                    long limiteAte = Math.min(ate, ultimoEscrito);

                    List<SensorRecord> registos = file.range(de, limiteAte);
                    for (SensorRecord r : registos) {
                        out.println(r.toLine());
                    }
                    out.println("FIM");

                    // O log usa o 'ate' pedido. O tamanho da lista indica o servido.
                    System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + s.getInetAddress().getHostAddress() + " (" + registos.size() + " registos)");

                } catch (Exception e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }
}