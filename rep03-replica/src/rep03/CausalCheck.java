package rep03;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class CausalCheck {
    private static final int LIMITE_TABELA = 12;
    private static final int LIMITE_EXEMPLOS = 5;

    private static final class Evento {
        final int k;
        final SensorRecord r;

        Evento(int k, SensorRecord r) {
            this.k = k;
            this.r = r;
        }

        String nome() {
            return "p" + k + "#" + r.getSeq();
        }

        String descricao() {
            return nome() + " " + r.getSensorId() + " " + r.getTemperature();
        }

        long l() {
            return r.getLamport();
        }

        VectorClock v() {
            return r.getVector();
        }

        boolean alarme() {
            return r.getSensorId().startsWith(Wire.PREFIXO_ALARME);
        }
    }

    private static final Comparator<Evento> ORDEM_LAMPORT =
            Comparator.comparingLong(Evento::l).thenComparingInt(e -> e.k);

    private final int n;
    private final File pasta;
    private final String endereco;
    private final List<List<Evento>> particoes = new ArrayList<>();
    private final StringBuilder rel = new StringBuilder();

    private long causais;
    private long concorrentes;
    private long concorrentesLDistinto;
    private long violacoesRelogio;
    private int totalAlarmes;
    private double regSem;
    private double regCom;

    private CausalCheck(int n, File pasta, String endereco) {
        this.n = n;
        this.pasta = pasta;
        this.endereco = endereco;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta> [endereço]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        File pasta = new File(args[1]);
        if (!pasta.isDirectory()) {
            System.out.println("Erro: a pasta " + pasta + " não existe ou não é um diretório.");
            return;
        }
        new CausalCheck(n, pasta, args.length > 2 ? args[2] : Wire.ENDERECO_LOCAL).executar();
    }

    private void executar() throws IOException {
        p("=== CausalCheck · N = " + n + " · pasta " + pasta.getAbsolutePath() + " ===");
        verificarFicheiros();
        List<Evento> todos = new ArrayList<>();
        particoes.forEach(todos::addAll);
        if (todos.isEmpty()) {
            p("Sem registos para analisar.");
            terminar();
            return;
        }
        classificarPares();
        analisarAlarmes();
        medirMensagens(todos);
        escreverVistaLamport(todos);
        analisarEscritasReplicas();
        if (todos.size() <= LIMITE_TABELA) {
            tabelaDePares(todos);
        }
        resumo();
        terminar();
    }

    private void verificarFicheiros() {
        p("");
        p("1 · Ficheiros dos líderes");
        for (int k = 0; k < n; k++) {
            List<Evento> eventos = new ArrayList<>();
            particoes.add(eventos);
            File f = new File(pasta, "lider-p" + k + ".txt");
            if (!f.exists()) {
                p("  " + f.getName() + ": não encontrado (partição tratada como vazia)");
                continue;
            }

            List<String> problemas = new ArrayList<>();
            int linhasMalFormadas = 0;
            List<String> linhas;
            try {
                linhas = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                p("  " + f.getName() + ": erro de leitura (" + e.getMessage() + ")");
                continue;
            }
            for (String linha : linhas) {
                if (linha.isBlank()) {
                    continue;
                }
                try {
                    eventos.add(new Evento(k, SensorRecord.fromLine(linha, n)));
                } catch (RuntimeException e) {
                    linhasMalFormadas++;
                    if (problemas.size() < LIMITE_EXEMPLOS) {
                        problemas.add("linha inválida (" + e.getMessage() + ")");
                    }
                }
            }

            boolean vkIgualSeq = true;
            boolean seqContinua = true;
            boolean crescentes = true;
            for (int i = 0; i < eventos.size(); i++) {
                Evento e = eventos.get(i);
                if (e.v().get(k) != e.r.getSeq()) {
                    vkIgualSeq = false;
                    if (problemas.size() < LIMITE_EXEMPLOS) {
                        problemas.add(e.nome() + ": V[" + k + "]=" + e.v().get(k) + " != seq=" + e.r.getSeq());
                    }
                }
                if (e.r.getSeq() != i + 1) {
                    seqContinua = false;
                }
                if (i > 0) {
                    Evento ant = eventos.get(i - 1);
                    if (ant.l() >= e.l() || ant.v().compare(e.v()) != VectorClock.Order.BEFORE) {
                        crescentes = false;
                        if (problemas.size() < LIMITE_EXEMPLOS) {
                            problemas.add(ant.nome() + " (L=" + ant.l() + " V=" + ant.v() + ") não precede "
                                    + e.nome() + " (L=" + e.l() + " V=" + e.v() + ")");
                        }
                    }
                }
            }
            long alarmes = eventos.stream().filter(Evento::alarme).count();
            p("  " + f.getName() + ": " + eventos.size() + " registos (" + alarmes + " alarmes)");
            p("    - linhas válidas (6 campos, vetor de dimensão " + n + "): "
                    + (linhasMalFormadas == 0 ? "todas" : linhasMalFormadas + " inválidas, ignoradas"));
            p("    - V[" + k + "] = seq em todas as linhas: " + simNao(vkIgualSeq));
            p("    - seq de 1 a " + eventos.size() + " sem saltos nem repetições: " + simNao(seqContinua));
            p("    - L e V estritamente crescentes ao longo do ficheiro: " + simNao(crescentes));
            for (String pr : problemas) {
                p("      ! " + pr);
            }
        }
    }

    private void classificarPares() {
        long aAntesB = 0;
        long bAntesA = 0;
        long iguais = 0;
        List<String> violacoes = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                for (Evento a : particoes.get(i)) {
                    for (Evento b : particoes.get(j)) {
                        switch (a.v().compare(b.v())) {
                            case BEFORE:
                                aAntesB++;
                                verificarRelogio(a, b, violacoes);
                                break;
                            case AFTER:
                                bAntesA++;
                                verificarRelogio(b, a, violacoes);
                                break;
                            case CONCURRENT:
                                concorrentes++;
                                if (a.l() != b.l()) {
                                    concorrentesLDistinto++;
                                }
                                break;
                            default:
                                iguais++;
                        }
                    }
                }
            }
        }
        causais = aAntesB + bAntesA;
        long total = causais + concorrentes + iguais;

        p("");
        p("2 · Pares de registos de partições diferentes (a na partição de menor índice)");
        p("  pares analisados: " + total);
        p("  a → b: " + aAntesB);
        p("  b → a: " + bAntesA);
        p("  causalmente relacionados (total): " + causais + " (" + pct(causais, total) + ")");
        p("  concorrentes: " + concorrentes + " (" + pct(concorrentes, total) + ")");
        if (iguais > 0) {
            p("  ! com vetores iguais (impossível em eventos distintos): " + iguais);
        }

        p("");
        p("3 · Condição de relógio: a → b ⇒ L(a) < L(b)");
        p("  pares causais verificados: " + causais);
        p("  violações: " + violacoesRelogio + (violacoesRelogio == 0 ? " (condição verificada)" : ""));
        for (String v : violacoes) {
            p("      ! " + v);
        }

        p("");
        p("4 · Relógio de Lamport perante a concorrência");
        p("  pares concorrentes com L distinto: " + concorrentesLDistinto + " de " + concorrentes
                + " (" + pct(concorrentesLDistinto, concorrentes) + ")");
        p("    → o relógio de Lamport ordena-os, embora não exista relação causal entre eles");
        p("  pares concorrentes com L igual: " + (concorrentes - concorrentesLDistinto)
                + " (ordenados apenas pelo número da partição)");
    }

    private void verificarRelogio(Evento causa, Evento efeito, List<String> violacoes) {
        if (causa.l() >= efeito.l()) {
            violacoesRelogio++;
            if (violacoes.size() < LIMITE_EXEMPLOS) {
                violacoes.add(causa.descricao() + " → " + efeito.descricao() + " mas L=" + causa.l()
                        + " >= L=" + efeito.l() + "  (V " + causa.v() + " < " + efeito.v() + ")");
            }
        }
    }

    private void analisarAlarmes() {
        Map<String, List<Evento>> porSensor = new HashMap<>();
        List<Evento> alarmes = new ArrayList<>();
        for (List<Evento> lista : particoes) {
            for (Evento e : lista) {
                if (e.alarme()) {
                    alarmes.add(e);
                } else {
                    porSensor.computeIfAbsent(e.r.getSensorId(), s -> new ArrayList<>()).add(e);
                }
            }
        }
        totalAlarmes = alarmes.size();

        int comOrigem = 0;
        int outraParticao = 0;
        List<String> semOrigem = new ArrayList<>();
        for (Evento c : alarmes) {
            String sensor = c.r.getSensorId().substring(Wire.PREFIXO_ALARME.length());
            Evento origem = null;
            for (Evento a : porSensor.getOrDefault(sensor, new ArrayList<>())) {
                if (a.r.getTemperature() == c.r.getTemperature() && a.v().compare(c.v()) == VectorClock.Order.BEFORE) {
                    origem = a;
                    break;
                }
            }
            if (origem != null) {
                comOrigem++;
                if (origem.k != c.k) {
                    outraParticao++;
                }
            } else if (semOrigem.size() < LIMITE_EXEMPLOS) {
                semOrigem.add(c.descricao() + " V=" + c.v());
            }
        }

        p("");
        p("5 · Alarmes");
        p("  alarmes registados pelos líderes: " + totalAlarmes);
        if (totalAlarmes > 0) {
            p("  com origem causal identificada (leitura do mesmo sensor e temperatura com V < V(alarme)): " + comOrigem);
            p("    dos quais com a origem noutra partição: " + outraParticao);
            p("  sem origem causal identificada: " + (totalAlarmes - comOrigem));
            for (String s : semOrigem) {
                p("      ! " + s);
            }
        }
    }

    private void medirMensagens(List<Evento> todos) {
        long somaSem = 0;
        long somaCom = 0;
        for (Evento e : todos) {
            SensorRecord r = e.r;
            String com = Wire.reg(endereco, Wire.portoRecuperacao(e.k), r);
            String sem = "REG;" + endereco + ";" + Wire.portoRecuperacao(e.k) + ";"
                    + r.getSeq() + ";" + r.getSensorId() + ";" + r.getTemperature() + ";" + r.getTimestamp();
            somaCom += com.getBytes(StandardCharsets.UTF_8).length;
            somaSem += sem.getBytes(StandardCharsets.UTF_8).length;
        }
        regSem = (double) somaSem / todos.size();
        regCom = (double) somaCom / todos.size();

        p("");
        p("6 · Tamanho das mensagens REG (endereço anunciado " + endereco + ")");
        p("  tamanho médio sem carimbos: " + fmt(regSem) + " bytes");
        p("  tamanho médio com carimbos: " + fmt(regCom) + " bytes");
        p("  custo médio dos carimbos: " + fmt(regCom - regSem) + " bytes por mensagem ("
                + pct(Math.round((regCom - regSem) * 1000), Math.round(regSem * 1000)) + " a mais)");
    }

    private void escreverVistaLamport(List<Evento> todos) throws IOException {
        List<Evento> ordenados = new ArrayList<>(todos);
        ordenados.sort(ORDEM_LAMPORT);
        StringBuilder sb = new StringBuilder();
        for (Evento e : ordenados) {
            sb.append(e.l()).append(";p").append(e.k).append(";").append(e.r.toLine()).append(System.lineSeparator());
        }
        File f = new File(pasta, "vista-lamport.txt");
        Files.writeString(f.toPath(), sb.toString(), StandardCharsets.UTF_8);

        p("");
        p("7 · Ordem total por (L, k)");
        p("  " + ordenados.size() + " registos escritos em " + f.getName() + " (formato L;p<k>;seq;sensor;temperatura;instante;L;V)");
    }

    private void analisarEscritasReplicas() {
        File[] diarios = pasta.listFiles((d, nome) -> nome.matches("replica-\\d+-escritas\\.txt"));
        p("");
        p("8 · Ordem de escrita nas réplicas (replica-<id>-escritas.txt)");
        if (diarios == null || diarios.length == 0) {
            p("  nenhum ficheiro de escritas encontrado");
            return;
        }
        Arrays.sort(diarios);
        for (File f : diarios) {
            List<Object[]> escritas = new ArrayList<>();
            try {
                for (String linha : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
                    if (linha.isBlank()) {
                        continue;
                    }
                    String[] c = linha.split(";", 4);
                    int k = Integer.parseInt(c[1].substring(1));
                    escritas.add(new Object[] { Long.parseLong(c[0]), new Evento(k, SensorRecord.fromLine(c[3], n)), c[2] });
                }
            } catch (IOException | RuntimeException e) {
                p("  " + f.getName() + ": não foi possível ler (" + e.getMessage() + ")");
                continue;
            }

            long foraDeOrdem = 0;
            long alarmesAntesDaOrigem = 0;
            List<String> exemplos = new ArrayList<>();
            for (int i = 0; i < escritas.size(); i++) {
                Evento x = (Evento) escritas.get(i)[1];
                for (int j = i + 1; j < escritas.size(); j++) {
                    Evento y = (Evento) escritas.get(j)[1];
                    if (x.k == y.k || y.v().compare(x.v()) != VectorClock.Order.BEFORE) {
                        continue;
                    }
                    foraDeOrdem++;
                    boolean origemDoAlarme = x.alarme()
                            && x.r.getSensorId().equals(Wire.PREFIXO_ALARME + y.r.getSensorId())
                            && x.r.getTemperature() == y.r.getTemperature();
                    if (origemDoAlarme) {
                        alarmesAntesDaOrigem++;
                    }
                    if (origemDoAlarme && exemplos.size() < LIMITE_EXEMPLOS) {
                        exemplos.add(x.descricao() + " escrito às " + hora((Long) escritas.get(i)[0])
                                + "; a sua origem " + y.descricao() + " só às " + hora((Long) escritas.get(j)[0])
                                + " (via " + escritas.get(j)[2] + ")");
                    }
                }
            }
            p("  " + f.getName() + ": " + escritas.size() + " escritas");
            p("    - pares escritos antes de uma causa sua de outra partição: " + foraDeOrdem);
            p("    - alarmes escritos antes da leitura que os originou: " + alarmesAntesDaOrigem);
            for (String s : exemplos) {
                p("      · " + s);
            }
        }
    }

    private void tabelaDePares(List<Evento> todos) {
        List<Evento> ev = new ArrayList<>(todos);
        ev.sort(Comparator.comparingInt((Evento e) -> e.k).thenComparingLong(e -> e.r.getSeq()));
        p("");
        p("Relações entre todos os pares (" + ev.size() + " registos)");
        p(String.format("  %-28s | %-24s | %-8s | %-22s | %s", "Par", "V", "L", "Relação causal", "Ordem por (L, k)"));
        for (int i = 0; i < ev.size(); i++) {
            for (int j = i + 1; j < ev.size(); j++) {
                Evento a = ev.get(i);
                Evento b = ev.get(j);
                String par = a.nome() + " " + a.r.getSensorId() + ", " + b.nome() + " " + b.r.getSensorId();
                String v;
                String rel;
                switch (a.v().compare(b.v())) {
                    case BEFORE:
                        v = "(" + a.v() + ") < (" + b.v() + ")";
                        rel = a.nome() + " → " + b.nome();
                        break;
                    case AFTER:
                        v = "(" + a.v() + ") > (" + b.v() + ")";
                        rel = b.nome() + " → " + a.nome();
                        break;
                    case CONCURRENT:
                        v = "incomparáveis";
                        rel = a.nome() + " ∥ " + b.nome();
                        break;
                    default:
                        v = "iguais";
                        rel = "?";
                }
                String l = a.l() + (a.l() < b.l() ? " < " : a.l() > b.l() ? " > " : " = ") + b.l();
                Evento primeiro = ORDEM_LAMPORT.compare(a, b) < 0 ? a : b;
                Evento segundo = primeiro == a ? b : a;
                p(String.format("  %-28s | %-24s | %-8s | %-22s | %s", par, v, l, rel,
                        primeiro.nome() + " antes de " + segundo.nome()));
            }
        }
    }

    private void resumo() {
        StringBuilder porParticao = new StringBuilder();
        for (int k = 0; k < n; k++) {
            if (k > 0) porParticao.append(" / ");
            porParticao.append(particoes.get(k).size());
        }
        p("");
        p("RESUMO: registos " + porParticao + " | alarmes " + totalAlarmes + " | pares causais " + causais
                + " | concorrentes " + concorrentes + " | concorrentes com L distinto " + concorrentesLDistinto
                + " (" + pct(concorrentesLDistinto, concorrentes) + ") | violações da condição de relógio "
                + violacoesRelogio + " | REG sem/com carimbos " + fmt(regSem) + " / " + fmt(regCom) + " bytes");
    }

    private void p(String s) {
        rel.append(s).append(System.lineSeparator());
    }

    private void terminar() throws IOException {
        System.out.print(rel);
        File f = new File(pasta, "relatorio-causal.txt");
        Files.writeString(f.toPath(), rel.toString(), StandardCharsets.UTF_8);
        System.out.println("(relatório guardado em " + f.getPath() + ")");
    }

    private static String simNao(boolean b) {
        return b ? "Sim" : "Não";
    }

    private static String pct(long parte, long total) {
        return total == 0 ? "—" : String.format(Locale.ROOT, "%.1f %%", 100.0 * parte / total);
    }

    private static String fmt(double d) {
        return String.format(Locale.ROOT, "%.1f", d);
    }

    private static String hora(long millis) {
        return LocalTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()).toString();
    }
}
