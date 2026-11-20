package raft04;

/** Pedido de voto: PEDIDO_VOTO;termo;candidato;ultimoSeq;ultimoTermo. */
public final class VoteRequest {
    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        if (term < 0) {
            throw new IllegalArgumentException("Termo negativo: " + term);
        }
        if (candidate < 1 || candidate > Wire.GRUPO) {
            throw new IllegalArgumentException("Candidato inválido: " + candidate);
        }
        if (lastSeq < 0 || lastTerm < 0) {
            throw new IllegalArgumentException("Último seq e último termo devem ser não negativos");
        }
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String message) {
        if (message == null) {
            throw new IllegalArgumentException("Pedido de voto nulo");
        }
        String[] fields = message.trim().split(";", -1);
        if (fields.length != 5 || !"PEDIDO_VOTO".equals(fields[0])) {
            throw new IllegalArgumentException("Formato PEDIDO_VOTO inválido");
        }
        try {
            return new VoteRequest(
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]),
                    Long.parseLong(fields[3]),
                    Long.parseLong(fields[4]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Números inválidos em PEDIDO_VOTO", e);
        }
    }
}
