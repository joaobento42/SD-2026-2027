package tpf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public record Registo(long lsn, long instante, String autor, String canal, String texto) {

    public String paraLinhaLog() {
        String textoBase64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        return lsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoBase64;
    }

    public static Registo daLinhaLog(String linha) {
        String[] campos = linha.split(";", 5);
        String texto = new String(Base64.getDecoder().decode(campos[4]), StandardCharsets.UTF_8);
        return new Registo(Long.parseLong(campos[0]), Long.parseLong(campos[1]), campos[2], campos[3], texto);
    }
}
