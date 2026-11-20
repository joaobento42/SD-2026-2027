package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class RaftState {
    public enum Role { SEGUIDOR, CANDIDATO, LIDER }

    // Resposta a um pedido de voto: termo conhecido e decisão.
    public static final class Vote {
        public final long term;
        public final boolean granted;

        public Vote(long term, boolean granted) {
            this.term = term;
            this.granted = granted;
        }
    }

    private long term = 0;
    private int votedFor = 0; // 0 significa que ainda não votou neste termo.
    private final int self;
    private Role role = Role.SEGUIDOR;
    private final Set<Integer> votes = new HashSet<>();
    private int knownLeader = 0; // 0 significa que não reconhecemos um líder neste termo.
    private long deadline;
    private final Path stateFile;

    public RaftState(Path stateFile, int self) throws IOException {
        checkNodeId(self);
        this.stateFile = stateFile;
        this.self = self;

        // Recupera o termo e o voto para não votar duas vezes após reiniciar.
        if (Files.exists(stateFile)) {
            String line = Files.readString(stateFile, StandardCharsets.UTF_8).trim();
            String[] fields = line.split(";", -1);
            if (fields.length != 2) {
                throw new IOException("Estado RAFT mal formado: " + stateFile);
            }
            long loadedTerm;
            int loadedVote;
            try {
                loadedTerm = Long.parseLong(fields[0]);
                loadedVote = Integer.parseInt(fields[1]);
            } catch (NumberFormatException e) {
                throw new IOException(
                        "Estado persistido inválido: termo ou voto não é um número válido", e);
            }
            if (loadedTerm < 0 || loadedVote < 0 || loadedVote > Wire.GRUPO) {
                throw new IOException("Estado persistido inválido: termo ou voto fora dos limites");
            }
            term = loadedTerm;
            votedFor = loadedVote;
        }
        resetDeadline();
    }

    // Rejeita nós que não pertencem ao grupo de réplicas.
    private static void checkNodeId(int nodeId) {
        if (nodeId < 1 || nodeId > Wire.GRUPO) {
            throw new IllegalArgumentException("ID de nó inválido: " + nodeId);
        }
    }

    // Sorteia o prazo de eleição configurado e guarda-o como instante monotónico.
    private void resetDeadline() {
        long min = Wire.ELEICAO_MIN_MS;
        long max = Wire.ELEICAO_MAX_MS;
        if (min <= 0 || max < min || max == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Limites de eleição inválidos: " + min + ".." + max);
        }
        long timeout = ThreadLocalRandom.current().nextLong(min, max + 1);
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    // Grava termo e voto no mesmo ficheiro, com substituição atómica.
    private void persist(long termToSave, int voteToSave) throws IOException {
        Path destination = stateFile.toAbsolutePath();
        Path temporary = Files.createTempFile(destination.getParent(), "raft-", ".tmp");
        try {
            Files.writeString(temporary, termToSave + ";" + voteToSave,
                    StandardCharsets.UTF_8);
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    // Ao conhecer um termo superior, guarda-o ANTES de mudar o estado em memória.
    private void adoptTerm(long newTerm) throws IOException {
        if (newTerm <= term) {
            return;
        }
        persist(newTerm, 0);
        term = newTerm;
        votedFor = 0;
        role = Role.SEGUIDOR;
        votes.clear();
        knownLeader = 0;
    }

    // Se o prazo expirou, inicia candidatura com o próprio voto já persistido.
    public synchronized long tick() throws IOException {
        if (role == Role.LIDER || System.nanoTime() < deadline) {
            return -1;
        }
        if (term == Long.MAX_VALUE) {
            throw new IllegalStateException("Termo RAFT atingiu o limite de long");
        }

        long nextTerm = term + 1;
        persist(nextTerm, self);

        term = nextTerm;
        votedFor = self;
        role = Role.CANDIDATO;
        knownLeader = 0;
        votes.clear();
        votes.add(self);
        resetDeadline();
        return term;
    }

    // Vota apenas num candidato elegível e com registo suficientemente atualizado.
    public synchronized Vote onVoteRequest(
            long candidateTerm,
            int candidate,
            long candidateLastSeq,
            long candidateLastTerm,
            long myLastSeq,
            long myLastTerm
    ) throws IOException {
        checkNodeId(candidate);
        if (candidateTerm < 0 || candidateLastSeq < 0 || candidateLastTerm < 0
                || myLastSeq < 0 || myLastTerm < 0) {
            throw new IllegalArgumentException("Termos e posições do registo não podem ser negativos");
        }

        if (candidateTerm < term) {
            return new Vote(term, false);
        }
        if (candidateTerm > term) {
            adoptTerm(candidateTerm);
        }

        boolean canVote = votedFor == 0 || votedFor == candidate;
        boolean logIsUpToDate = candidateLastTerm > myLastTerm
                || (candidateLastTerm == myLastTerm && candidateLastSeq >= myLastSeq);

        if (!canVote || !logIsUpToDate) {
            return new Vote(term, false);
        }

        // Primeiro guardar, só depois responder que o voto foi concedido.
        if (votedFor != candidate) {
            persist(term, candidate);
            votedFor = candidate;
        }
        resetDeadline();
        return new Vote(term, true);
    }

    // Conta votos únicos da candidatura atual e sinaliza a passagem a líder.
    public synchronized boolean onVote(
            long voteTerm,
            int voter,
            int candidate,
            boolean granted
    ) throws IOException {
        checkNodeId(voter);
        checkNodeId(candidate);
        if (voteTerm < 0) {
            throw new IllegalArgumentException("Termo do voto não pode ser negativo");
        }

        if (voteTerm > term) {
            adoptTerm(voteTerm);
            return false;
        }
        if (voteTerm < term || role != Role.CANDIDATO || candidate != self || !granted) {
            return false;
        }

        votes.add(voter); // Set ignora respostas repetidas do mesmo eleitor.
        if (votes.size() >= Wire.GRUPO / 2 + 1) {
            role = Role.LIDER;
            knownLeader = self;
            return true;
        }
        return false;
    }

    // Aceita lotes do líder reconhecido, mas não de termos anteriores.
    // O conteúdo do lote tem de ser validado pelo chamador antes deste método.
    public synchronized boolean onBatch(long batchTerm, int batchLeader) throws IOException {
        checkNodeId(batchLeader);
        if (batchTerm < 0) {
            throw new IllegalArgumentException("Termo do lote não pode ser negativo");
        }
        if (batchTerm < term) {
            return false;
        }
        if (batchTerm > term) {
            adoptTerm(batchTerm);
        }

        // Dois líderes diferentes não podem ser aceites no mesmo termo.
        if (knownLeader != 0 && knownLeader != batchLeader) {
            return false;
        }
        if (role == Role.LIDER && batchLeader != self) {
            return false;
        }

        knownLeader = batchLeader;
        if (role != Role.LIDER) {
            role = Role.SEGUIDOR;
            votes.clear();
            resetDeadline();
        }
        return true;
    }

    public synchronized long getTerm() {
        return term;
    }

    public synchronized Role getRole() {
        return role;
    }

    public synchronized int getKnownLeader() {
        return knownLeader;
    }
}
