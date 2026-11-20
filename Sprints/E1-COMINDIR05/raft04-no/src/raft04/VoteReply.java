package raft04;

/** Resposta de voto: VOTO;termo;eleitor;candidato;S|N. */
public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        if (term < 0) {
            throw new IllegalArgumentException("Termo negativo: " + term);
        }
        if (voter < 1 || voter > Wire.GRUPO) {
            throw new IllegalArgumentException("Eleitor inválido: " + voter);
        }
        if (candidate < 1 || candidate > Wire.GRUPO) {
            throw new IllegalArgumentException("Candidato inválido: " + candidate);
        }
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String message) {
        if (message == null) {
            throw new IllegalArgumentException("Resposta de voto nula");
        }
        String[] fields = message.trim().split(";", -1);
        if (fields.length != 5 || !"VOTO".equals(fields[0])) {
            throw new IllegalArgumentException("Formato VOTO inválido");
        }
        if (!"S".equals(fields[4]) && !"N".equals(fields[4])) {
            throw new IllegalArgumentException("Decisão de voto inválida (esperado S ou N)");
        }
        try {
            return new VoteReply(
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]),
                    Integer.parseInt(fields[3]),
                    "S".equals(fields[4]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Números inválidos em VOTO", e);
        }
    }
}
