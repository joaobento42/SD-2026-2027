package rep03;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class LeaderClient implements AutoCloseable {
    private final String[] hosts;
    private final Ligacao[] ligacoes;

    public LeaderClient(int n, String hostsArg) {
        this.hosts = new String[n];
        this.ligacoes = new Ligacao[n];
        String[] h = (hostsArg == null || hostsArg.isBlank())
                ? new String[] { Wire.ENDERECO_LOCAL } : hostsArg.split(",");
        if (h.length != 1 && h.length != n) {
            throw new IllegalArgumentException("Indique 1 ou " + n + " endereços de líderes: " + hostsArg);
        }
        for (int k = 0; k < n; k++) {
            hosts[k] = (h.length == 1 ? h[0] : h[k]).trim();
        }
    }

    public String endereco(int k) {
        return hosts[k] + ":" + Wire.portoEntrada(k);
    }

    public synchronized void send(int k, String msg) throws IOException {
        Ligacao l = ligacoes[k];
        if (l != null && !l.fechada) {
            try {
                l.enviar(msg);
                return;
            } catch (IOException e) {
            }
        }
        fechar(k);
        l = new Ligacao(hosts[k], Wire.portoEntrada(k));
        ligacoes[k] = l;
        try {
            l.enviar(msg);
        } catch (IOException e) {
            fechar(k);
            throw e;
        }
    }

    private void fechar(int k) {
        if (ligacoes[k] != null) {
            ligacoes[k].fechar();
            ligacoes[k] = null;
        }
    }

    @Override
    public synchronized void close() {
        for (int k = 0; k < ligacoes.length; k++) {
            fechar(k);
        }
    }

    private static final class Ligacao {
        private final Socket socket;
        private final Writer out;
        private volatile boolean fechada;

        Ligacao(String host, int porto) throws IOException {
            socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(host, porto), Wire.T_TCP_MS);
                socket.setTcpNoDelay(true);
                out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                socket.close();
                throw new IOException("líder inacessível em " + host + ":" + porto + " (" + e.getMessage() + ")", e);
            }
            Thread vigia = new Thread(this::vigiar, "vigia-" + host + ":" + porto);
            vigia.setDaemon(true);
            vigia.start();
        }

        private void vigiar() {
            try {
                InputStream in = socket.getInputStream();
                while (in.read() != -1) {
                }
            } catch (IOException ignorada) {
            }
            fechada = true;
        }

        void enviar(String msg) throws IOException {
            if (fechada) {
                throw new IOException("ligação fechada pelo líder");
            }
            out.write(msg);
            out.write('\n');
            out.flush();
        }

        void fechar() {
            fechada = true;
            try {
                socket.close();
            } catch (IOException ignorada) {
            }
        }
    }
}
