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

public class LeaderBot extends ListenerAdapter {

    public static void main(String[] args) throws InterruptedException {
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("GUILD_ID");

        if (token == null || token.isBlank() || guildId == null || guildId.isBlank()) {
            System.err.println("ERRO: Defina as variáveis de ambiente DISCORD_TOKEN e GUILD_ID antes de arrancar.");
            System.exit(1);
        }

        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot())
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

    // Verifica se o membro possui o papel "admin-arquivo"
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
                event.reply("/registar recebido: texto=\"" + texto + "\" — ainda não implementado").queue();
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
                event.reply("/rajada recebido: n=" + n + ", intervaloMs=" + intervaloMs + " — ainda não implementado").queue();
            }

            default -> event.reply("Comando desconhecido.").setEphemeral(true).queue();
        }
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        // Ignora mensagens enviadas por bots (incluindo o próprio LeaderBot)
        if (event.getAuthor().isBot()) {
            return;
        }

        // Verifica se a mensagem foi enviada no canal #arquivo
        if (event.getChannel().getName().equals("arquivo")) {
            String autor = event.getAuthor().getName();
            String conteudo = event.getMessage().getContentDisplay();

            System.out.println("[#arquivo] Mensagem detetada de " + autor + ": " + conteudo);
            event.getChannel().sendMessage("[Provisório] Mensagem detetada em `#arquivo` de **" + autor + "**: `" + conteudo + "` — ainda não implementado").queue();
        }
    }
}