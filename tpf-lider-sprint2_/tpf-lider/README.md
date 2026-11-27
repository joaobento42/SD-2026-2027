# tpf-lider — Líder do arquivo distribuído (sprints E2-01 e E2-02)

Bot Discord (JDA) que atua como líder do arquivo. Cada `/registar` e cada mensagem em `#arquivo`
recebe um LSN, é gravado em `registos.log` com `fsync` antes da resposta e entra no chunk aberto.
O chunk é selado por tamanho ou por tempo, serializado com padding, cifrado com AES-256-GCM e
gravado em `selados/`, com o nome igual ao SHA-256 do conteúdo cifrado.

Os comandos `/estado`, `/config`, `/recuperar`, `/replica-token` e `/replica-remover` mantêm a
resposta provisória do sprint E2-01.

## Pré-requisitos
- Java 17 ou superior e Maven (o IntelliJ traz um)
- Aplicação e bot no Discord Developer Portal, com o *Privileged Intent* **Message Content** ativado
- Servidor Discord com o canal `#arquivo` e o papel `admin-arquivo`

## Variáveis de ambiente
- `DISCORD_TOKEN`: token do bot (Developer Portal).
- `GUILD_ID`: ID do servidor de teste onde os *slash commands* são registados.

## Arrancar no IntelliJ
1. Abrir o `pom.xml` de `tpf-lider` como projeto Maven e esperar pelas dependências.
2. *Run → Edit Configurations → + → Application*: classe `tpf.LeaderBot`, variáveis de ambiente
   `DISCORD_TOKEN=...;GUILD_ID=...` e, se quiseres, VM options como `-Dtpf.selagemMs=5000`.
3. *Run*. A consola mostra `Pasta do líder: <caminho>` e cada evento à medida que acontece.

Parâmetros (`-Dtpf.*`): `tpf.chunkMaxBytes` (65536), `tpf.padBytes` (4096), `tpf.selagemMs` (5000).

## Ficheiros da pasta do líder (`dados-lider/`)

| Ficheiro | Conteúdo |
|---|---|
| `master.key` | 32 bytes de `SecureRandom`, gerada no primeiro arranque, só de leitura para o dono (`r--------` em Linux/macOS; ACL só com o dono em Windows). Se existir com outro tamanho, o líder recusa arrancar. |
| `registos.log` | uma linha por registo: `lsn;instante;autor;canal;texto em Base64` (`instante` em ms desde 1970, UTC) |
| `eventos-lider.txt` | `instante;evento;detalhe`: `REGISTO` (`lsn`, `autor`, `canal`) e `SELADO` (`chunkId`, `lsn=de-ate`, `tamanho` com padding, `motivo=tamanho` ou `tempo`) |
| `selados/<chunkId>.bin` | texto cifrado do chunk, seguido da tag GCM (16 bytes) |
| `selados/<chunkId>.hdr` | `keyVersion;salt;nonce` — `keyVersion` em decimal (sempre 1), `salt` (32 bytes) e `nonce` (12 bytes) em Base64 |

## Formato binário do chunk

Inteiros em *big-endian*; cada texto é precedido do seu comprimento em bytes (UTF-8).

```
nRegistos  int (4 bytes)
nRegistos vezes:
  lsn       long (8 bytes)
  instante  long (8 bytes)
  autor     int n (4 bytes) + n bytes UTF-8
  canal     int n (4 bytes) + n bytes UTF-8
  texto     int n (4 bytes) + n bytes UTF-8
padding    bytes 0x00 até o total ser múltiplo de PAD_BYTES
```

A leitura inversa (`Chunk.ler`) lê `nRegistos` registos e ignora o resto, que é padding.

## Selagem
- O chunk é selado quando o tamanho serializado chega a `CHUNK_MAX_BYTES`. O registo que faz o
  chunk exceder o limite fica nesse chunk e sela-o de imediato, por isso um chunk pode passar dos
  65 536 bytes em menos de um registo; com o padding, o tamanho continua múltiplo de `PAD_BYTES`.
- Também é selado `SELAGEM_MS` depois do seu primeiro registo (`ScheduledExecutorService`). Cada
  chunk tem uma "geração": se já foi selado por tamanho quando o temporizador dispara, o
  temporizador não faz nada, para não selar antes do tempo o chunk seguinte.
- Ao selar: `salt` (32 bytes) e `nonce` (12 bytes) aleatórios; `DEK = HKDF-SHA256(MK, salt,
  "tpf-chunk-v1")` (extração e expansão de um bloco); AES-256-GCM com tag de 128 bits e
  `aad = "tpf-v1" + keyVersion (4 bytes) + salt + nonce`; `chunkId` = SHA-256 do cifrado com a tag.
- `open` verifica por esta ordem: hash contra o `chunkId`, versão da chave e autenticação GCM.

## Testes de `ChunkCrypto`
No IntelliJ: abrir `src/test/java/tpf/ChunkCryptoTest.java` e carregar no triângulo verde ao lado
da classe. São os 6 testes pedidos: cifrar e decifrar (e ler de volta os mesmos registos), byte
alterado no texto cifrado, `salt`, `nonce` e `keyVersion` diferentes e `master.key` diferente.

## Testar no Discord
Apaga a pasta `dados-lider` para partir do zero, arranca o líder e deixa a consola visível.
Os comandos `grep`, `sha256sum` e `ls` correm no Git Bash.

1. **`/registar texto: olá arquivo`** → `Registado com LSN 1.` A linha `1;...;b2zDoSBhcnF1aXZv`
   já está em `dados-lider/registos.log` (critério 1).
2. **Mensagem em `#arquivo`** → o bot responde `Mensagem de ... registada com LSN 2.` Uma mensagem
   noutro canal não é registada, nem as respostas do bot (critério 2).
3. **Parar e voltar a arrancar** o líder e fazer `/registar` → o LSN é o seguinte ao último do log
   (critério 3).
4. **Esperar 5 s** depois de um registo → `eventos-lider.txt` mostra `SELADO ... motivo=tempo` cerca
   de 5 s depois do `REGISTO` (critério 4).
5. **`/rajada n: 500 intervaloms: 0`** (com o papel `admin-arquivo`) → vários
   `SELADO ... motivo=tamanho` e, 5 s depois, um `motivo=tempo` com o resto.
   `ls -l dados-lider/selados/*.bin` → todos os tamanhos menos 16 (a tag) são múltiplos de 4096 (critério 4).
6. **Nomes e texto em claro** (critério 5):
   - `sha256sum dados-lider/selados/*.bin` → cada hash é igual ao nome do ficheiro;
   - `grep -r "olá arquivo" dados-lider/selados/` → não devolve nada.
7. **Abrir os chunks e adulteração** (critérios 6 e 7): `ChunkCryptoTest`.
8. **Eventos** (critério 8): abrir `dados-lider/eventos-lider.txt` e mostrar as linhas `REGISTO` e `SELADO`.
