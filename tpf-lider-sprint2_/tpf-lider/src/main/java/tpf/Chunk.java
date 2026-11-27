package tpf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class Chunk {

    private final List<Registo> registos = new ArrayList<>();
    private int tamanho = 4;

    public void adicionar(Registo r) {
        registos.add(r);
        tamanho += 8 + 8 + 4 + utf8(r.autor()).length + 4 + utf8(r.canal()).length + 4 + utf8(r.texto()).length;
    }

    public boolean vazio() {
        return registos.isEmpty();
    }


    public int tamanho() {
        return tamanho;
    }

    public long lsnDe() {
        return registos.get(0).lsn();
    }

    public long lsnAte() {
        return registos.get(registos.size() - 1).lsn();
    }


    public byte[] serializar(int padBytes) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(registos.size());
            for (Registo r : registos) {
                out.writeLong(r.lsn());
                out.writeLong(r.instante());
                escreverTexto(out, r.autor());
                escreverTexto(out, r.canal());
                escreverTexto(out, r.texto());
            }
            int comPadding = ((tamanho + padBytes - 1) / padBytes) * padBytes;
            out.write(new byte[comPadding - tamanho]);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }


    public static List<Registo> ler(byte[] dados) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(dados));
        int n = in.readInt();
        List<Registo> lidos = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long lsn = in.readLong();
            long instante = in.readLong();
            lidos.add(new Registo(lsn, instante, lerTexto(in), lerTexto(in), lerTexto(in)));
        }
        return lidos;
    }

    private static void escreverTexto(DataOutputStream out, String s) throws IOException {
        byte[] b = utf8(s);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String lerTexto(DataInputStream in) throws IOException {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
