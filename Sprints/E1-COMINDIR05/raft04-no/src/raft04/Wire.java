package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public final class Wire {
    // Período de heartbeat configurável, com o valor pedido nesta ficha.
    public static final long PERIODO_HB_MS =
            Long.getLong("raft04.periodo", 2000L);
    public static final int FATOR_TIMEOUT =
            Integer.parseInt(System.getProperty("raft04.fator", "3"));
    public static final long VERIFICACAO_MS = 200L;
    // Limites configuráveis do prazo aleatório de eleição.
    public static final long ELEICAO_MIN_MS =
            Long.getLong("raft04.eleicaoMin", 3L * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS =
            Long.getLong("raft04.eleicaoMax", 6L * PERIODO_HB_MS);
    public static final int GRUPO = 3;
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 10000;
    public static final double LIMIAR_ALARME = 28.0;
    public static final String PREFIXO_ALARME = "ALARM-";

    static {
        if (PERIODO_HB_MS <= 0 || FATOR_TIMEOUT < 1) {
            throw new IllegalArgumentException("raft04.periodo e raft04.fator têm de ser positivos");
        }
    }

    private Wire() { }

    public static String topicIn(int k) { return "raft04-in-p" + k; }
    public static String topicData(int k) { return "raft04-p" + k; }
    // Tópico de pedidos e respostas de eleição da partição.
    public static String topicElection(int k) {
        checkPartition(k);
        return "raft04-eleicao-p" + k;
    }

    // Devolve um porto TCP único para cada nó das três partições.
    public static int nodePort(int k, int i) {
        checkPartition(k);
        checkNode(i);
        return PORTO_BASE + k * GRUPO + (i - 1);
    }

    // O ficheiro de sinalização simula a separação de rede deste nó.
    public static boolean isolated(int k, int i) {
        checkPartition(k);
        checkNode(i);
        return Files.isRegularFile(Path.of("isolar-p" + k + "-n" + i));
    }

    private static void checkPartition(int k) {
        if (k < 0 || k >= GRUPO) {
            throw new IllegalArgumentException("Partição inválida: " + k);
        }
    }

    private static void checkNode(int i) {
        if (i < 1 || i > GRUPO) {
            throw new IllegalArgumentException("ID de nó inválido: " + i);
        }
    }

    // IN;sensor;temp;L;V
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    public static String type(String message) {
        String trimmed = message.trim();
        int separator = trimmed.indexOf(';');
        if (separator <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + message);
        }
        return trimmed.substring(0, separator);
    }

    public static String body(String message) {
        String trimmed = message.trim();
        int separator = trimmed.indexOf(';');
        if (separator < 0) {
            throw new IllegalArgumentException("Mensagem sem conteúdo: " + message);
        }
        return trimmed.substring(separator + 1);
    }

}
