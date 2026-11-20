package raft04;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RaftStateTest {
    private static int passed;

    private static void check(boolean condition, String name) {
        if (!condition) {
            throw new AssertionError(name);
        }
        passed++;
    }

    private static void expectInvalidState(Path dir, String name, String content)
            throws IOException {
        Path stateFile = dir.resolve(name);
        Files.writeString(stateFile, content);
        try {
            new RaftState(stateFile, 1);
            throw new AssertionError("Estado inválido aceite: " + name);
        } catch (IOException expected) {
            passed++;
        }
    }

    private static long longField(RaftState state, String name) throws Exception {
        Field field = RaftState.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(state);
    }

    private static Object field(RaftState state, String name) throws Exception {
        Field field = RaftState.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(state);
    }

    private static void forceDeadline(RaftState state) throws Exception {
        Field field = RaftState.class.getDeclaredField("deadline");
        field.setAccessible(true);
        field.setLong(state, System.nanoTime() - 1L);
    }

    private static void checkDeadlineRange(RaftState state, long startedAt) throws Exception {
        long remaining = longField(state, "deadline") - startedAt;
        long min = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Wire.ELEICAO_MIN_MS);
        long max = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Wire.ELEICAO_MAX_MS);
        long tolerance = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(100);
        check(remaining >= min - tolerance && remaining <= max + tolerance,
                "deadline dentro dos limites configurados");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "invalid-deadline".equals(args[0])) {
            try {
                new RaftState(Path.of("unused-state.txt"), 1);
                System.exit(2);
            } catch (IllegalArgumentException expected) {
                return;
            }
        }

        check(Wire.nodePort(0, 1) == 10000, "primeiro porto TCP");
        check(Wire.nodePort(2, 3) == 10008, "último porto TCP");
        try {
            Wire.nodePort(-1, 1);
            throw new AssertionError("partição TCP inválida aceite");
        } catch (IllegalArgumentException expected) {
            passed++;
        }
        try {
            Wire.nodePort(0, 4);
            throw new AssertionError("ID de nó TCP inválido aceite");
        } catch (IllegalArgumentException expected) {
            passed++;
        }

        Path dir = Files.createTempDirectory("raft-state-test-");
        Path stateFile = dir.resolve("state.txt");
        long startedAt = System.nanoTime();
        RaftState state = new RaftState(stateFile, 1);
        check(state.getTerm() == 0, "getter de termo inicial");
        check(state.getRole() == RaftState.Role.SEGUIDOR, "getter de papel inicial");
        check(state.getKnownLeader() == 0, "getter de líder inicial");
        checkDeadlineRange(state, startedAt);
        check(state.tick() == -1, "tick espera pelo prazo");

        forceDeadline(state);
        check(state.tick() == 1, "primeira candidatura no termo 1");
        check(state.getTerm() == 1, "getter de termo candidato");
        check(state.getRole() == RaftState.Role.CANDIDATO, "getter de papel candidato");
        check("1;1".equals(Files.readString(stateFile)), "voto próprio persistido");
        checkDeadlineRange(state, System.nanoTime());

        check(!state.onVote(0, 2, 1, true), "voto de termo antigo ignorado");
        check(!state.onVote(1, 2, 3, true), "voto para outro candidato ignorado");
        check(!state.onVote(1, 2, 1, false), "recusa ignorada");
        check(state.onVote(1, 2, 1, true), "maioria 2/3 elege líder");
        check(state.getRole() == RaftState.Role.LIDER, "getter de papel líder");
        check(state.getKnownLeader() == 1, "getter de líder eleito");
        check(!state.onVote(1, 2, 1, true), "voto duplicado não reelege");
        check(state.tick() == -1, "líder não se candidata");
        check(!state.onBatch(0, 2), "lote antigo rejeitado");
        check(!state.onBatch(1, 2), "líder conflituante no mesmo termo rejeitado");
        check(state.onBatch(2, 2), "lote de termo novo adota líder e rebaixa");
        check(state.getTerm() == 2, "getter de termo adotado");
        check(state.getKnownLeader() == 2, "getter de novo líder reconhecido");
        check("2;0".equals(Files.readString(stateFile)), "adoção de termo persistida");
        check(field(state, "role") == RaftState.Role.SEGUIDOR, "nó voltou a seguidor");
        check(!state.onBatch(2, 3), "segundo líder no mesmo termo rejeitado");
        check(state.onBatch(2, 2), "lote do líder reconhecido aceite");

        RaftState restored = new RaftState(stateFile, 1);
        check(longField(restored, "term") == 2L, "termo recuperado do disco");
        check((int) field(restored, "votedFor") == 0, "voto recuperado do disco");
        RaftState.Vote stale = restored.onVoteRequest(2, 2, 10, 2, 12, 2);
        check(!stale.granted && stale.term == 2L, "registo mais antigo não recebe voto");
        RaftState.Vote current = restored.onVoteRequest(2, 2, 13, 2, 12, 2);
        check(current.granted, "registo atualizado recebe voto");
        check("2;2".equals(Files.readString(stateFile)), "voto concedido persistido");
        check(restored.onVoteRequest(2, 2, 13, 2, 12, 2).granted,
                "mesmo candidato pode repetir pedido");
        check(!restored.onVoteRequest(2, 3, 100, 3, 12, 2).granted,
                "não vota em dois candidatos no mesmo termo");
        RaftState.Vote newerTermStaleLog = restored.onVoteRequest(3, 3, 1, 1, 12, 2);
        check(!newerTermStaleLog.granted && newerTermStaleLog.term == 3,
                "adota termo superior mesmo com registo desatualizado");
        check("3;0".equals(Files.readString(stateFile)), "termo novo persistido sem voto");
        check(restored.onVoteRequest(3, 3, 0, 3, 12, 2).granted,
                "termo de registo superior prevalece sobre seq inferior");
        check("3;3".equals(Files.readString(stateFile)), "voto no novo termo persistido");
        RaftState again = new RaftState(stateFile, 1);
        check((int) field(again, "votedFor") == 3, "voto sobrevive ao reinício");

        try {
            new RaftState(dir.resolve("invalid-self.txt"), 0);
            throw new AssertionError("ID próprio inválido aceite");
        } catch (IllegalArgumentException expected) {
            passed++;
        }
        try {
            again.onVote(3, 4, 1, true);
            throw new AssertionError("ID eleitor inválido aceite");
        } catch (IllegalArgumentException expected) {
            passed++;
        }

        expectInvalidState(dir, "bad-format.txt", "1");
        expectInvalidState(dir, "bad-number.txt", "abc;1");
        expectInvalidState(dir, "negative-term.txt", "-1;1");
        expectInvalidState(dir, "negative-vote.txt", "1;-1");
        expectInvalidState(dir, "large-vote.txt", "1;" + (Wire.GRUPO + 1));

        checkInvalidDeadline("0", "12000");
        checkInvalidDeadline("12001", "12000");

        System.out.println("OK: " + passed + " verificações passaram");
    }

    private static void checkInvalidDeadline(String min, String max) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classPath = System.getProperty("java.class.path");
        Process process = new ProcessBuilder(java,
                "-Draft04.eleicaoMin=" + min,
                "-Draft04.eleicaoMax=" + max,
                "-cp", classPath,
                RaftStateTest.class.getName(),
                "invalid-deadline")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        check(exitCode == 0, "limites eleitorais inválidos rejeitados: " + output);
    }
}
