package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

public class LeaderBot extends ListenerAdapter {

    public static void main(String[] args) throws InterruptedException {
        // Lê o token e o ID do servidor a partir das variáveis de ambiente
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("GUILD_ID");

        if (token == null || token.isBlank() || guildId == null || guildId.isBlank()) {
            System.err.println("ERRO: Defina as variáveis de ambiente DISCORD_TOKEN e GUILD_ID antes de arrancar.");
            System.exit(1);
        }

        // Inicializa o JDA com o intent Message Content ativado
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot())
                .build();

        // Aguarda que o bot fique totalmente ligado
        jda.awaitReady();

        // Obtém o servidor de teste e regista os Guild Commands (atualização imediata)
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
                            .addOption(OptionType.INTEGER, "ate", "Índice final", true)
            ).queue();
            System.out.println("Comandos registados com sucesso no servidor: " + guild.getName());
        } else {
            System.err.println("ERRO: Servidor com ID " + guildId + " não encontrado.");
        }
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String comando = event.getName();

        switch (comando) {
            case "registar" -> {
                String texto = event.getOption("texto").getAsString();
                event.reply("[Provisório] Comando `/registar` recebido com texto: `" + texto + "`").queue();
            }
            case "estado" -> {
                event.reply("[Provisório] Comando `/estado` recebido. Sistema operacional.").queue();
            }
            case "config" -> {
                if ("r".equals(event.getSubcommandName())) {
                    int n = event.getOption("n").getAsInt();
                    event.reply("[Provisório] Comando `/config r` recebido com n = `" + n + "`").queue();
                }
            }
            case "recuperar" -> {
                int de = event.getOption("de").getAsInt();
                int ate = event.getOption("ate").getAsInt();
                event.reply("[Provisório] Comando `/recuperar` recebido de `" + de + "` até `" + ate + "`").queue();
            }
            default -> event.reply("Comando desconhecido.").setEphemeral(true).queue();
        }
    }
}