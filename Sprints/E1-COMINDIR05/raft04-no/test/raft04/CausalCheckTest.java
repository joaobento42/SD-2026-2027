package raft04;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Testes unitarios isolados, sem IPFS/Kubo. */
public final class CausalCheckTest {
    private static int passed;

    private static void check(boolean value, String description) {
        if (!value) throw new AssertionError(description);
        passed++;
    }

    private static String line(long seq, long term, String sensor, long lamport, String vector) {
        return new SensorRecord(seq, term, sensor, 21.5, "123456", lamport,
                VectorClock.parse(vector, 3)).toLine();
    }

    private static Path fixture() throws Exception {
        Path folder = Files.createTempDirectory("causalcheck-test-");
        String[] lines = {
                line(1, 2, "s0", 1, "1,0,0"),
                line(1, 2, "s1", 2, "1,1,0"),
                line(1, 2, "s2", 3, "0,0,1")
        };
        for (int k = 0; k < 3; k++) {
            for (int i = 1; i <= 3; i++) {
                write(folder, k, i, lines[k]);
            }
        }
        return folder;
    }

    private static void write(Path folder, int k, int i, String... lines) throws Exception {
        Files.write(folder.resolve("no-p" + k + "-n" + i + ".txt"),
                List.of(lines), StandardCharsets.UTF_8);
    }

    private record Result(int errors, String output, List<String> view) { }

    private static Result run(Path folder) throws Exception {
        PrintStream old = System.out;
        ByteArrayOutputStream capture = new ByteArrayOutputStream();
        int errors;
        try (PrintStream stream = new PrintStream(capture, true, StandardCharsets.UTF_8)) {
            System.setOut(stream);
            errors = CausalCheck.check(3, folder);
        } finally {
            System.setOut(old);
        }
        List<String> view = Files.readAllLines(folder.resolve("vista-lamport.txt"),
                StandardCharsets.UTF_8);
        return new Result(errors, capture.toString(StandardCharsets.UTF_8), view);
    }

    public static void main(String[] args) throws Exception {
        Path valid = fixture();
        Result r = run(valid);
        check(r.errors == 0, "replicas identicas sem erros");
        check(r.view.size() == 3, "3 eventos unicos na vista");
        check(r.output.contains("copias=6"), "6 copias nao contadas como eventos");
        check(r.output.contains("causais=1"), "relacao causal p0 -> p1");
        check(r.output.contains("concorrentes=2"), "restantes pares concorrentes");

        Path differentContent = fixture();
        write(differentContent, 0, 2, line(1, 2, "s0", 1, "1,1,0"));
        r = run(differentContent);
        check(r.errors > 0, "conflito com mesma identidade assinalado");
        check(r.output.contains("DIVERGENCIA DE CONTEUDO"), "conteudo divergente descrito");
        check(r.view.size() == 2, "identidade divergente fora da vista");
        check(r.output.contains("p0") && r.output.contains("n2"), "no identificado");

        Path differentTerm = fixture();
        write(differentTerm, 0, 3, line(1, 3, "s0", 1, "1,0,0"));
        r = run(differentTerm);
        check(r.errors > 0, "termos divergentes assinalados");
        check(r.output.contains("DIVERGENCIA DE TERMO p0 seq=1"), "divergencia de posicao descrita");
        check(r.output.contains("termo=2 nos=[no-p0-n1.txt, no-p0-n2.txt]"),
                "termo 2 associado aos nos 1 e 2");
        check(r.output.contains("termo=3 nos=[no-p0-n3.txt]"),
                "termo 3 associado ao no 3");
        check(r.view.size() == 2, "posicao com dois termos excluida");
        check(r.view.stream().noneMatch(text -> text.contains("p0#1")),
                "a posicao divergente nao entra na vista causal");

        Path wrongVectorLength = fixture();
        write(wrongVectorLength, 2, 1, line(1, 2, "s2", 3, "0,0,1").replace("0,0,1", "0,1"));
        r = run(wrongVectorLength);
        check(r.errors > 0, "tamanho de vetor invalido identificado");
        check(r.output.contains("vetor com 2 posicoes"), "mensagem de dimensao clara");
        check(r.view.size() == 2, "vetor errado excluido dos calculos");

        Path wrongVectorValue = fixture();
        write(wrongVectorValue, 2, 2, line(1, 2, "s2", 3, "0,0,2"));
        r = run(wrongVectorValue);
        check(r.errors > 0, "V[k] != seq detetado");
        check(r.output.contains("V[2]=2"), "violacao informa componente");
        check(r.view.size() == 2, "posicao com vetor errado excluida");

        Path lamportViolation = fixture();
        for (int i = 1; i <= 3; i++) {
            write(lamportViolation, 1, i, line(1, 2, "s1", 1, "1,1,0"));
        }
        r = run(lamportViolation);
        check(r.errors > 0, "violacao Lamport detetada");
        check(r.output.contains("ERRO LAMPORT"), "mensagem Lamport");

        Path equalVectors = fixture();
        for (int i = 1; i <= 3; i++) {
            write(equalVectors, 0, i, line(1, 2, "s0", 1, "1,1,0"));
        }
        r = run(equalVectors);
        check(r.errors > 0, "vetores iguais em eventos diferentes detetados");
        check(r.output.contains("mesmo vetor"), "mensagem de vetores iguais");

        Path missingFile = fixture();
        Files.delete(missingFile.resolve("no-p0-n3.txt"));
        r = run(missingFile);
        check(r.errors > 0, "ficheiro em falta detetado");
        check(r.output.contains("falta no-p0-n3.txt"), "mensagem de ficheiro em falta");
        check(r.view.size() == 3, "eventos existentes ainda analisados");

        Path malformed = fixture();
        write(malformed, 1, 3, "linha invalida");
        r = run(malformed);
        check(r.errors > 0, "linha malformada detetada");
        check(r.output.contains("linha 1"), "linha malformada identificada");
        check(r.view.size() == 3, "outras copias utilizaveis");

        Path sequenceGap = fixture();
        // As tres replicas concordam: a unica diferenca e a lacuna entre seq=1 e seq=3.
        for (int i = 1; i <= 3; i++) {
            write(sequenceGap, 0, i,
                    line(1, 2, "s0", 1, "1,0,0"),
                    line(3, 2, "s0", 4, "3,0,0"));
        }
        r = run(sequenceGap);
        check(r.errors == 0, "lacuna isolada nao causa erro");
        check(!r.output.contains("sequencia nao continua"),
                "lacuna nao e reportada como violacao");
        check(r.view.size() == 4, "evento apos a lacuna continua na vista causal");
        check(r.view.stream().anyMatch(text -> text.contains("p0#3(t2)")),
                "evento apos a lacuna e preservado");

        System.out.println("CausalCheckTest: " + passed + " verificacoes passaram.");
    }
}
