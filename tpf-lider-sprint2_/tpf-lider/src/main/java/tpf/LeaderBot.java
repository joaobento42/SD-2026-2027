package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;



public class LeaderBot extends ListenerAdapter implements AutoCloseable {

    static final int TAMANHO_TEXTO_RAJADA = 400;

    private final Path pastaSelados;
    private final int chunkMaxBytes;
    private final int padBytes;
    private final long selagemMs;
    private final ChunkCrypto crypto;
    private final FileChannel log;
    private final BufferedWriter eventos;
    private final ScheduledExecutorService temporizador = Executors.newSingleThreadScheduledExecutor();

    private long ultimoLsn;
    private Chunk chunkAberto = new Chunk();
    private long geracao;

    public LeaderBot(Path pasta) throws IOException, GeneralSecurityException {
        this(pasta, Wire.CHUNK_MAX_BYTES, Wire.PAD_BYTES, Wire.SELAGEM_MS);
    }

    public LeaderBot(Path pasta, int chunkMaxBytes, int padBytes, long selagemMs)
            throws IOException, GeneralSecurityException {
        this.chunkMaxBytes = chunkMaxBytes;
        this.padBytes = padBytes;
        this.selagemMs = selagemMs;
        this.pastaSelados = pasta.resolve("selados");
        Files.createDirectories(pastaSelados);


        this.crypto = new ChunkCrypto(ChunkCrypto.carregarOuGerarMasterKey(pasta.resolve("master.key")));


        Path ficheiroLog = pasta.resolve("registos.log");
        if (Files.exists(ficheiroLog)) {
            List<String> linhas = Files.readAllLines(ficheiroLog, StandardCharsets.UTF_8);
            if (!linhas.isEmpty()) {
                ultimoLsn = Registo.daLinhaLog(linhas.get(linhas.size() - 1)).lsn();
            }
        }
        this.log = FileChannel.open(ficheiroLog, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        this.eventos = Files.newBufferedWriter(pasta.resolve("eventos-lider.txt"), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }


    public synchronized Registo registar(String autor, String canal, String texto)
            throws IOException, GeneralSecurityException {
        Registo r = new Registo(ultimoLsn + 1, System.currentTimeMillis(), autor, canal, texto);
        log.write(ByteBuffer.wrap((r.paraLinhaLog() + "\n").getBytes(StandardCharsets.UTF_8)));
        log.force(true);
        ultimoLsn = r.lsn();
        evento("REGISTO", "lsn=" + r.lsn() + ",autor=" + autor + ",canal=" + canal);

        boolean primeiro = chunkAberto.vazio();
        chunkAberto.adicionar(r);
        if (primeiro) {
            long geracaoDoChunk = geracao;
            temporizador.schedule(() -> selarPorTempo(geracaoDoChunk), selagemMs, TimeUnit.MILLISECONDS);
        }
        if (chunkAberto.tamanho() >= chunkMaxBytes) {
            selar("tamanho");
        }
        return r;
    }


    private synchronized void selarPorTempo(long geracaoDoChunk) {

        if (geracaoDoChunk != geracao) {
            return;
        }
        try {
            selar("tempo");
        } catch (IOException | GeneralSecurityException e) {
            System.err.println("Erro ao selar o chunk: " + e);
        }
    }


    private void selar(String motivo) throws IOException, GeneralSecurityException {
        byte[] claro = chunkAberto.serializar(padBytes);
        ChunkCrypto.Sealed selado = crypto.seal(claro);
        selado.gravar(pastaSelados);
        evento("SELADO", "chunkId=" + selado.id() + ",lsn=" + chunkAberto.lsnDe() + "-" + chunkAberto.lsnAte()
                + ",tamanho=" + claro.length + ",motivo=" + motivo);
        chunkAberto = new Chunk();
        geracao++;
    }


    public void rajada(int n, long intervaloMs, String autor, String canal)
            throws IOException, GeneralSecurityException, InterruptedException {
        for (int i = 1; i <= n; i++) {
            String prefixo = "rajada " + i + "/" + n + " ";
            registar(autor, canal, prefixo + "x".repeat(TAMANHO_TEXTO_RAJADA - prefixo.length()));
            if (intervaloMs > 0) {
                Thread.sleep(intervaloMs);
            }
        }
    }


    private void evento(String tipo, String detalhe) throws IOException {
        String linha = Instant.now() + ";" + tipo + ";" + detalhe;
        System.out.println(linha);
        eventos.write(linha);
        eventos.newLine();
        eventos.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        temporizador.shutdownNow();
        log.close();
        eventos.close();
    }

    public static void main(String[] args) throws Exception {
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("GUILD_ID");

        if (token == null || token.isBlank() || guildId == null || guildId.isBlank()) {
            System.err.println("ERRO: Defina as variáveis de ambiente DISCORD_TOKEN e GUILD_ID antes de arrancar.");
            System.exit(1);
        }


        Path pasta = Path.of("dados-lider");
        LeaderBot bot = new LeaderBot(pasta);
        System.out.println("Pasta do líder: " + pasta.toAbsolutePath());

        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(bot)
                .build();

        jda.awaitReady();

        Guild guild = jda.getGuildById(guildId);
        if (guild != null) {
            guild.updateCommands().addCommands(
                    Commands.slash("registar", "Regista um novo texto no arquivo")
                            .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                    Commands.slash("estado", "Mostra o estado atual do sistema"),

                    Commands.slash("config", "Configurações do sistema")
                            .addSubcommands(
                                    new SubcommandData("r", "Define o fator de replicação (n)")
                                            .addOption(OptionType.INTEGER, "n", "Número de réplicas", true)
                            ),

                    Commands.slash("recuperar", "Recupera registos num intervalo")
                            .addOption(OptionType.INTEGER, "de", "Índice inicial", true)
                            .addOption(OptionType.INTEGER, "ate", "Índice final", true),

                    Commands.slash("replica-token", "Gera um token para uma nova réplica"),

                    Commands.slash("replica-remover", "Remove uma réplica do sistema")
                            .addOption(OptionType.STRING, "id", "Identificador da réplica", true),

                    Commands.slash("rajada", "Envia uma rajada de mensagens de teste")
                            .addOption(OptionType.INTEGER, "n", "Número de mensagens", true)
                            .addOption(OptionType.INTEGER, "intervaloms", "Intervalo entre mensagens em milissegundos", true)
            ).queue();
            System.out.println("Comandos registados com sucesso no servidor: " + guild.getName());
        } else {
            System.err.println("ERRO: Servidor com ID " + guildId + " não encontrado.");
        }
    }


    private boolean temPermissaoAdmin(Member member) {
        if (member == null) return false;
        return member.getRoles().stream()
                .anyMatch(role -> role.getName().equals("admin-arquivo"));
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String comando = event.getName();

        switch (comando) {
            case "registar" -> {
                String texto = event.getOption("texto").getAsString().trim();
                if (texto.isEmpty()) {
                    event.reply("Erro: O texto a registar não pode estar vazio.").setEphemeral(true).queue();
                    return;
                }
                try {
                    Registo r = registar(event.getUser().getName(), event.getChannel().getName(), texto);
                    event.reply("Registado com LSN " + r.lsn() + ".").queue();
                } catch (IOException | GeneralSecurityException e) {
                    event.reply("Erro ao registar: " + e.getMessage()).setEphemeral(true).queue();
                }
            }

            case "estado" -> {
                event.reply("/estado recebido — ainda não implementado").queue();
            }

            case "config" -> {
                if (!temPermissaoAdmin(event.getMember())) {
                    event.reply("Acesso negado: Apenas membros com o papel `admin-arquivo` podem usar `/config`.")
                            .setEphemeral(true).queue();
                    return;
                }
                if ("r".equals(event.getSubcommandName())) {
                    int n = event.getOption("n").getAsInt();
                    if (n < 1) {
                        event.reply("Erro: O fator de replicação `n` deve ser maior ou igual a 1.").setEphemeral(true).queue();
                        return;
                    }
                    event.reply("/config r recebido: n=" + n + " — ainda não implementado").queue();
                }
            }

            case "recuperar" -> {
                int de = event.getOption("de").getAsInt();
                int ate = event.getOption("ate").getAsInt();
                if (de < 1 || ate < de) {
                    event.reply("Erro: Intervalo inválido (`de` deve ser >= 1 e `ate` >= `de`).").setEphemeral(true).queue();
                    return;
                }
                event.reply("/recuperar recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
            }

            case "replica-token" -> {
                if (!temPermissaoAdmin(event.getMember())) {
                    event.reply("Acesso negado: Apenas membros com o papel `admin-arquivo` podem usar `/replica-token`.")
                            .setEphemeral(true).queue();
                    return;
                }
                event.reply("/replica-token recebido — ainda não implementado").setEphemeral(true).queue();
            }

            case "replica-remover" -> {
                if (!temPermissaoAdmin(event.getMember())) {
                    event.reply("Acesso negado: Apenas membros com o papel `admin-arquivo` podem usar `/replica-remover`.")
                            .setEphemeral(true).queue();
                    return;
                }
                String id = event.getOption("id").getAsString().trim();
                if (id.isEmpty()) {
                    event.reply("Erro: O `id` da réplica não pode estar vazio.").setEphemeral(true).queue();
                    return;
                }
                event.reply("/replica-remover recebido: id=" + id + " — ainda não implementado").queue();
            }

            case "rajada" -> {
                if (!temPermissaoAdmin(event.getMember())) {
                    event.reply("Acesso negado: Apenas membros com o papel `admin-arquivo` podem usar `/rajada`.")
                            .setEphemeral(true).queue();
                    return;
                }
                int n = event.getOption("n").getAsInt();
                int intervaloMs = event.getOption("intervaloms").getAsInt();
                if (n < 1 || intervaloMs < 0) {
                    event.reply("Erro: `n` deve ser >= 1 e `intervaloMs` >= 0.").setEphemeral(true).queue();
                    return;
                }
                String autor = event.getUser().getName();
                String canal = event.getChannel().getName();

                event.reply("Rajada iniciada: " + n + " registos com intervalo de " + intervaloMs + " ms.").queue();
                new Thread(() -> {
                    try {
                        rajada(n, intervaloMs, autor, canal);
                    } catch (Exception e) {
                        System.err.println("Rajada interrompida: " + e);
                    }
                }).start();
            }

            default -> event.reply("Comando desconhecido.").setEphemeral(true).queue();
        }
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {

        if (event.getAuthor().isBot()) {
            return;
        }


        if (event.getChannel().getName().equals("arquivo")) {
            String autor = event.getAuthor().getName();
            String conteudo = event.getMessage().getContentDisplay();
            try {
                Registo r = registar(autor, "arquivo", conteudo);
                event.getChannel().sendMessage("Mensagem de " + autor + " registada com LSN " + r.lsn() + ".").queue();
            } catch (IOException | GeneralSecurityException e) {
                event.getChannel().sendMessage("Erro ao registar: " + e.getMessage()).queue();
            }
        }
    }
}
