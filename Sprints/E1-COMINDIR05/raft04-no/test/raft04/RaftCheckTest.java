package raft04;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Testes offline com ficheiros sintéticos; não precisam de IPFS/Kubo. */
public final class RaftCheckTest {
    private static int checks;
    private RaftCheckTest() { }

    private record Result(int errors, String output) { }

    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
        checks++;
    }

    private static Result run(Path folder, Path failures) throws IOException {
        PrintStream saved = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            int errors = RaftCheck.check(3, folder, failures);
            return new Result(errors, bytes.toString(StandardCharsets.UTF_8));
        } finally {
            System.setOut(saved);
        }
    }

    private static void write(Path folder, String name, String content) throws IOException {
        Files.writeString(folder.resolve(name), content, StandardCharsets.UTF_8);
    }

    private static String record(int seq, int term, String sensor, double temp) {
        VectorClock v = new VectorClock(3);
        for (int i = 0; i < seq; i++) v.tick(0);
        return new SensorRecord(seq, term, sensor, temp, "2000", seq, v).toLine() + "\n";
    }

    private static Path fixture(boolean start) throws IOException {
        Path folder = Files.createTempDirectory("raftcheck-test-");
        for (int k = 0; k < 3; k++) {
            for (int i = 1; i <= 3; i++) {
                write(folder, "no-p" + k + "-n" + i + ".txt",
                        k == 0 ? record(1, 1, "S01", 20) + record(2, 2, "S03", 22) : "");
                write(folder, "estado-p" + k + "-n" + i + ".txt", k == 0 && i == 2 ? "2;2" : (k == 0 ? "2;0" : "1;0"));
                write(folder, "hb-p" + k + "-n" + i + ".txt", "");
                write(folder, "eventos-p" + k + "-n" + i + ".txt", "");
            }
        }
        write(folder, "eventos-p0-n1.txt", (start ? "1000;ARRANQUE;p0;\n" : "")
                + "1600;CANDIDATO;p0;termo=1;seq=0\n"
                + "1700;LIDER;p0;termo=1;n=1\n"
                + "3500;TRUNCADO;p0;de=3;descartados=1;termos=1\n");
        write(folder, "eventos-p0-n2.txt",
                "1601;VOTO;p0;eleitor=n2;candidato=n1;termo=1;concedido\n"
                + "2500;TERMO;p0;de=1;para=2;origem=CANDIDATURA\n"
                + "2500;CANDIDATO;p0;termo=2;seq=1\n"
                + "2600;LIDER;p0;termo=2;n=2\n");
        write(folder, "eventos-p0-n3.txt", "2505;VOTO;p0;eleitor=n3;candidato=n2;termo=2;concedido\n");
        write(folder, "hb-p0-n1.txt", "1;1;1720;0;0;0;60\n");
        write(folder, "hb-p0-n2.txt", "2;1;2700;2;2;1;140\n");
        write(folder, "produtor.txt", "1800;p0;S01;20.0\n2200;p0;S02;21.0\n2800;p0;S03;22.0\n");
        write(folder, "monitor-produtor.txt", "1900;NOVO_LIDER;p0;termo=1;lider=n1\n");
        write(folder, "monitor-processador.txt", "2750;NOVO_LIDER;p0;termo=2;lider=n2\n");
        write(folder, "falhas.txt", "p0;n1;2000\n");
        return folder;
    }

    public static void main(String[] args) throws Exception {
        Path folder = fixture(true);
        Result clean = run(folder, folder.resolve("falhas.txt"));
        if (clean.errors != 0) System.err.println(clean.output);
        check(clean.errors == 0, "cenário base sem violações");
        check(clean.output.contains("TERMO 1"), "termo 1 identificado");
        check(clean.output.contains("TERMO 2"), "termo 2 identificado");
        check(clean.output.contains("n1=2 voto(s)"), "contabiliza voto próprio e voto n2");
        check(clean.output.contains("n2=2 voto(s)"), "contabiliza voto próprio e voto n3");
        check(clean.output.contains("PRIMEIRO LÍDER: 700 ms"), "tempo a partir de ARRANQUE observado");
        check(clean.output.contains("indisponibilidade=700 ms"), "duração da falha medida pelo primeiro hb de termo superior");
        check(clean.output.contains("IN enviados no intervalo=1"), "leituras durante falha");
        check(clean.output.contains("LEITURAS PERDIDAS: 1 / 3"), "contagem por conteúdo com multiplicidade");
        check(clean.output.contains("TRUNCAGEM: operações=1 | registos descartados=1"), "truncagem contabilizada");
        check(clean.output.contains("COINCIDÊNCIA FINAL: seq divergentes até minUltimoSeq=2: []"), "três nós coincidem");
        check(clean.output.contains("TRUNCADOS REAPARECIDOS COM OUTRO seq: N/D"), "não inventa correspondências truncadas");

        write(folder, "eventos-p0-n3.txt", Files.readString(folder.resolve("eventos-p0-n3.txt"))
                + "2601;LIDER;p0;termo=2;n=3\n");
        Result twoLeaders = run(folder, null);
        check(twoLeaders.errors >= 1, "deteta dois líderes no mesmo termo");
        check(twoLeaders.output.contains("mais de um líder [2, 3]"), "identifica os dois nós líderes");

        folder = fixture(true);
        write(folder, "eventos-p0-n2.txt", Files.readString(folder.resolve("eventos-p0-n2.txt"))
                + "2900;TERMO;p0;de=2;para=1;origem=BUG\n");
        Result regress = run(folder, null);
        check(regress.errors >= 1, "deteta termo decrescente");
        check(regress.output.contains("termo diminuiu"), "explica a regressão de termo");

        folder = fixture(true);
        write(folder, "no-p0-n3.txt", record(1, 1, "S01", 20) + record(2, 1, "S03", 22));
        Result divergence = run(folder, null);
        check(divergence.errors >= 1, "deteta divergência final");
        check(divergence.output.contains("p0 seq=2"), "identifica a posição divergente");

        folder = fixture(false);
        Result unknownStart = run(folder, null);
        check(unknownStart.errors == 0, "sem ARRANQUE não é violação");
        check(unknownStart.output.contains("PRIMEIRO LÍDER: N/D"), "não estima instante não registado");

        folder = fixture(true);
        write(folder, "produtor.txt", "1800;p0;S01;20.0\n1810;p0;S01;20.0\n");
        Result duplicates = run(folder, null);
        check(duplicates.output.contains("LEITURAS PERDIDAS: 1 / 2"), "leituras idênticas contam com multiplicidade");

        folder = fixture(true);
        write(folder, "estado-p0-n1.txt", "-2;7");
        Result invalid = run(folder, null);
        check(invalid.errors >= 1, "estado inválido é detetado");

        folder = fixture(true);
        write(folder, "hb-p0-n2.txt", "2;1;2700\n");
        Result badHeartbeat = run(folder, null);
        check(badHeartbeat.errors >= 1, "heartbeat incompleto detetado");

        folder = fixture(true);
        write(folder, "hb-p0-n1.txt", "2;1;1800;2;2;1;100\n1;2;1900;2;1;0;60\n");
        Result hbRegression = run(folder, null);
        check(hbRegression.errors >= 1, "deteta termo decrescente no histórico de heartbeat");
        check(hbRegression.output.contains("hb-p0-n1.txt: termo diminuiu"), "identifica o ficheiro heartbeat");

        folder = fixture(true);
        write(folder, "eventos-p0-n3.txt", "2505;VOTO;p0;eleitor=n99;candidato=n2;termo=2;concedido\n");
        Result invalidVoter = run(folder, null);
        check(invalidVoter.errors >= 1, "rejeita voto registado com ID inválido");
        check(invalidVoter.output.contains("voto concedido com eleitor/candidato inválido"), "explica voto inválido");

        // A origem da métrica é o primeiro ARRANQUE de qualquer partição.
        folder = fixture(true);
        write(folder, "eventos-p1-n3.txt", "500;ARRANQUE;p1;n=3\n");
        Result globalStart = run(folder, null);
        check(globalStart.errors == 0, "arranque de outra partição não é violação");
        check(globalStart.output.contains("PRIMEIRO LÍDER: 1200 ms desde o primeiro ARRANQUE global"),
                "tempo do primeiro líder usa o ARRANQUE global da partição p1");

        // Uma falha de seguidor não implica indisponibilidade do grupo.
        folder = fixture(true);
        write(folder, "falhas.txt", "p0;n3;2000\n");
        Result followerFailure = run(folder, folder.resolve("falhas.txt"));
        check(followerFailure.errors == 0, "falha de seguidor não introduz erro fictício");
        check(followerFailure.output.contains("falha de seguidor; último líder observado=n1 termo=1"),
                "identifica corretamente o seguidor e o líder do termo ativo");
        check(followerFailure.output.contains("indisponibilidade da partição não demonstrada"),
                "não inventa indisponibilidade durante falha de seguidor");

        // Um ex-líder já substituído deve ser tratado como seguidor quando falha.
        folder = fixture(true);
        write(folder, "falhas.txt", "p0;n1;3000\n");
        Result formerLeaderFailure = run(folder, folder.resolve("falhas.txt"));
        check(formerLeaderFailure.output.contains("falha de seguidor; último líder observado=n2 termo=2"),
                "classifica o papel no instante da falha, não no histórico total do nó");

        // Um termo sem LIDER é indicado mesmo com um único candidato.
        folder = fixture(true);
        write(folder, "eventos-p1-n1.txt", "3000;TERMO;p1;de=1;para=2;origem=CANDIDATURA\n"
                + "3100;CANDIDATO;p1;termo=2;seq=0\n");
        write(folder, "estado-p1-n1.txt", "2;1");
        Result singleCandidate = run(folder, null);
        check(singleCandidate.errors == 0, "termo sem líder com candidato único não é violação de segurança");
        check(singleCandidate.output.contains("n1=1 voto(s)[1] | VOTO DIVIDIDO (sem LIDER observado; causa não comprovada)"),
                "termo sem líder com único candidato recebe indicação VOTO DIVIDIDO sem inferir empate");

        // A própria observação TERMO, sem candidatos ou votos, deve ser reportada.
        folder = fixture(true);
        write(folder, "eventos-p2-n1.txt", "3000;TERMO;p2;de=1;para=2;origem=LOTE\n");
        write(folder, "estado-p2-n1.txt", "2;0");
        Result termOnly = run(folder, null);
        check(termOnly.errors == 0, "termo observado sem candidatura não implica erro de segurança");
        String p2Section = termOnly.output.substring(termOnly.output.indexOf("=== PARTIÇÃO p2 ==="));
        check(p2Section.contains("TERMO 2 | candidatos: não observados | VOTO DIVIDIDO"),
                "o relatório inclui termos só conhecidos via TERMO");

        System.out.println("RaftCheckTest: " + checks + " verificações passaram.");
    }
}
