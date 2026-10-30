package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class PartitionLeader {
    private static final String FALHA = System.getProperty("falha", "");

    private final int k;
    private final int n;
    private final String endereco;
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final RecordFile file;

    private long seq;
    private long lamport;
    private VectorClock vector;

    private PartitionLeader(int k, int n, String endereco, String api) throws IOException {
        if (k < 0 || k >= n) {
            throw new IllegalArgumentException("Partição " + k + " fora de [0, " + (n - 1) + "]");
        }
        this.k = k;
        this.n = n;
        this.endereco = endereco;
        this.partitioner = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.file = new RecordFile("lider-p" + k + ".txt", n);

        SensorRecord ultimo = file.last();
        this.seq = ultimo == null ? 0 : ultimo.getSeq();
        if (ultimo == null || "reinicio-zero".equals(FALHA)) {
            this.lamport = 0;
            this.vector = new VectorClock(n);
        } else {
            this.lamport = ultimo.getLamport();
            this.vector = ultimo.getVector().copy();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        String endereco = args.length > 2 ? args[2] : Wire.ENDERECO_LOCAL;
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        new PartitionLeader(k, n, endereco, api).iniciar();
    }

    private void iniciar() throws IOException {
        ServerSocket servidorEntrada = new ServerSocket(Wire.portoEntrada(k));
        ServerSocket servidorRecuperacao = new ServerSocket(Wire.portoRecuperacao(k));

        synchronized (this) {
            System.out.println("Líder p" + k + " (N = " + n + "): entrada no porto " + Wire.portoEntrada(k)
                    + ", recuperação em " + endereco + ":" + Wire.portoRecuperacao(k));
            System.out.println("ARRANQUE: seq=" + seq + " L=" + lamport + " V=" + vector);
        }
        if (!FALHA.isEmpty()) {
            System.out.println("ATENÇÃO: falha simulada ativa: " + FALHA);
        }

        Thread entrada = new Thread(() -> servirEntrada(servidorEntrada), "entrada-p" + k);
        entrada.start();

        servirRecuperacao(servidorRecuperacao);
    }

    private void servirEntrada(ServerSocket servidor) {
        while (true) {
            try {
                Socket s = servidor.accept();
                Thread t = new Thread(() -> lerLigacao(s), "entrada-p" + k + "-" + s.getPort());
                t.start();
            } catch (IOException e) {
                System.out.println("Erro no servidor de entrada: " + e.getMessage());
            }
        }
    }

    private void lerLigacao(Socket s) {
        String origem = s.getInetAddress().getHostAddress() + ":" + s.getPort();
        System.out.println("ENTRADA: ligação de " + origem);
        try (Socket socket = s;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String linha;
            while ((linha = in.readLine()) != null) {
                if (!linha.isBlank()) {
                    processarIn(linha);
                }
            }
            System.out.println("ENTRADA: " + origem + " desligou");
        } catch (IOException e) {
            System.out.println("ENTRADA: ligação de " + origem + " interrompida: " + e.getMessage());
        }
    }

    private void processarIn(String msg) {
        Wire.In in;
        try {
            in = Wire.parseIn(msg, n);
        } catch (RuntimeException e) {
            System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
            return;
        }

        int j = partitioner.of(in.sensor);
        if (j != k) {
            System.out.println("PARTIÇÃO ERRADA: " + in.sensor + " pertence a p" + j);
            return;
        }

        SensorRecord r;
        try {
            r = registar(in);
        } catch (IOException e) {
            System.out.println("Erro ao gravar " + msg + ": " + e.getMessage());
            return;
        }

        try {
            ps.publish(Wire.topicData(k), Wire.reg(endereco, Wire.portoRecuperacao(k), r));
        } catch (Exception e) {
            System.out.println("Erro ao publicar seq=" + r.getSeq() + ": " + e.getMessage()
                    + " (as réplicas recuperam-no por TCP)");
        }
    }

    private synchronized SensorRecord registar(Wire.In in) throws IOException {
        long novoL = "lamport-local".equals(FALHA)
                ? lamport + 1
                : Math.max(lamport, in.lamport) + 1;

        VectorClock novoV = vector.copy();
        if (!"sem-merge".equals(FALHA)) {
            novoV.merge(in.vector);
        }
        novoV.tick(k);

        long novoSeq = seq + 1;

        if (novoV.get(k) != novoSeq) {
            System.out.println("VIOLAÇÃO: V[" + k + "]=" + novoV.get(k) + " != seq=" + novoSeq);
        }

        SensorRecord r = SensorRecord.now(novoSeq, in.sensor, in.temperature, novoL, novoV);
        file.append(r);

        seq = novoSeq;
        lamport = novoL;
        vector = novoV;

        System.out.println("EVENTO: seq=" + seq + " L=" + lamport + " V=" + vector
                + " (" + in.sensor + " " + in.temperature + ")");
        return r;
    }

    private synchronized long ultimoSeq() {
        return seq;
    }

    private void servirRecuperacao(ServerSocket servidor) throws IOException {
        try (ServerSocket server = servidor) {
            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    atenderPedido(in.readLine(), out, s.getInetAddress().getHostAddress());
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    private void atenderPedido(String pedido, PrintWriter out, String remoto) throws IOException {
        if (pedido == null || !pedido.startsWith("PEDIDO ")) {
            out.println("ERRO Pedido inválido");
            return;
        }
        String[] f = pedido.trim().split("\\s+");
        if (f.length != 3) {
            out.println("ERRO Formato incorreto");
            return;
        }
        long de;
        long ate;
        try {
            de = Long.parseLong(f[1]);
            ate = Long.parseLong(f[2]);
        } catch (NumberFormatException e) {
            out.println("ERRO Formato numérico incorreto");
            return;
        }
        if (de > ate) {
            out.println("ERRO Intervalo inválido");
            return;
        }

        long limite = Math.min(ate, ultimoSeq());
        List<SensorRecord> registos = file.range(de, limite);
        for (SensorRecord r : registos) {
            out.println(r.toLine());
        }
        out.println("FIM");
        System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + remoto + " (" + registos.size() + " registos)");
    }
}
