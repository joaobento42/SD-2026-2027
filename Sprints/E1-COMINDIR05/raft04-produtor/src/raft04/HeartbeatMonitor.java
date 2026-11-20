package raft04;

import java.util.Objects;
import java.util.function.Consumer;

/** Monitoriza o líder de uma partição pelo termo e pelo contador de heartbeats. */
public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;
    private State state = State.DESCONHECIDO;
    private long lastNanos;
    private long term = -1;        // -1: ainda não foi observado nenhum termo.
    private int leader = 0;        // 0: ainda não há líder conhecido.
    private long hb = 0;

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        if (partition < 0 || periodMs <= 0 || factor < 1) {
            throw new IllegalArgumentException("Parâmetros do monitor inválidos");
        }
        this.partition = partition;
        this.timeoutNanos = Math.multiplyExact(
                Math.multiplyExact(periodMs, factor), 1_000_000L);
        this.log = Objects.requireNonNull(log, "log");
        this.lastNanos = System.nanoTime();
    }

    public synchronized boolean onHeartbeat(long incomingTerm, int incomingLeader, long incomingHb) {
        if (incomingTerm < 0 || incomingLeader < 1 || incomingLeader > 3 || incomingHb < 1) {
            throw new IllegalArgumentException("Heartbeat inválido");
        }
        if (incomingTerm < term) {
            return false; // Um termo anterior nunca reativa um líder obsoleto.
        }
        if (incomingTerm == term) {
            if (incomingLeader != leader || incomingHb <= hb) {
                return false; // Líder conflituante, heartbeat duplicado ou atrasado.
            }
            if (incomingHb - hb > 1) {
                log.accept("HB_PERDIDO;p" + partition + ";termo=" + term
                        + ";lider=n" + leader + ";esperado=" + (hb + 1)
                        + ";recebido=" + incomingHb);
            }
        } else {
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + incomingTerm
                    + ";lider=n" + incomingLeader);
        }
        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";termo=" + incomingTerm
                    + ";lider=n" + incomingLeader + ";hb=" + incomingHb);
        }
        term = incomingTerm;
        leader = incomingLeader;
        hb = incomingHb;
        lastNanos = System.nanoTime();
        state = State.ATIVO;
        return true;
    }

    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";termo=" + term
                    + ";lider=n" + leader + ";hb=" + hb
                    + ";silencio_ms=" + elapsed / 1_000_000L);
        }
    }

    public synchronized State state() { return state; }
    public synchronized long term() { return term; }
    public synchronized int leader() { return leader; }
    public synchronized long heartbeat() { return hb; }
}
