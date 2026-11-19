# tpf-lider — Líder do Sistema de Arquivo Distribuído (Sprint E2-01)

Aplicação Java (JDA) que atua como líder/coordenador do sistema através de um bot Discord.

## Pré-requisitos
- Java 17 ou superior
- Maven 3.8+
- Aplicação e Bot configurados no Discord Developer Portal com o *Privileged Intent* **Message Content** ativado
- Servidor Discord com o canal `#arquivo` e o cargo `admin-arquivo`

## Configuração das Variáveis de Ambiente
Antes de arrancar o líder, defina as seguintes variáveis de ambiente:

- `DISCORD_TOKEN`: Token de autenticação do bot gerado no Discord Developer Portal.
- `GUILD_ID`: ID do servidor Discord de teste onde os *slash commands* serão registados.
