# Registos COMINDIR03

Condições dos ensaios: N = 3; réplicas 1 (p0, p1), 2 (p1, p2) e 3 (p2, p0); todos os processos e um
nó IPFS (Kubo 0.43.1) na mesma máquina (Windows 11, OpenJDK 27). Cada ensaio parte de ficheiros
vazios e termina com `CausalCheck`.

Nomenclatura: `p<k>#<seq>` é o registo com esse `seq` no líder da partição *k*.

## Decisões de implementação

**Entrada por TCP direto, em vez do tópico `rep03-in-p<k>`.** Cada mensagem `IN` tem um único
destinatário conhecido: o líder da partição do sensor. É, portanto, comunicação 1→1, e o PubSub não traz
vantagem nesse troço. O produtor e o processador enviam o `IN` por uma ligação TCP persistente ao
porto **7100 + *k*** do líder (classe `LeaderClient`). O formato da mensagem é o do enunciado
(`IN;<sensor>;<temperatura>;<L>;<V>`).

- O TCP entrega pela ordem de envio: as leituras de um sensor chegam ao líder pela ordem do produtor.
- Um líder parado é detetado no envio (`PERDIDA: … líder inacessível`), em vez de a leitura
  desaparecer sem rasto.
- Não há confirmação (ACK) do líder, por isso o troço continua sem numeração.

O PubSub fica para a difusão 1→N dos `REG` (tópico `rep03-p<k>`). A recuperação continua por TCP no
porto 7000 + *k*, como na COMINDIR02.

**Falhas do ponto 8 com `-Dfalha=<nome>`.** Em vez de editar e repor o código entre ensaios, as quatro
falhas que exigem alterar código estão escritas ao lado do código correto. Ativam-se com uma
propriedade de sistema, lida com `System.getProperty("falha", "")`. Sem a opção, nenhum ramo de falha é
seguido e o código é exatamente o da especificação. Com uma falha ativa, o processo escreve no arranque
`ATENÇÃO: falha simulada ativa: <nome>`.

| Valor | Processo | Efeito |
|---|---|---|
| `sem-merge` | `PartitionLeader.registar` | não executa `merge` com *V*<sub>msg</sub> |
| `lamport-local` | `PartitionLeader.registar` | usa `L = L + 1` em vez de `max(L, L_msg) + 1` |
| `reinicio-zero` | construtor de `PartitionLeader` | ignora *L* e *V* do último registo e começa com *L* = 0 e *V* = (0, …, 0) |
| `sem-copy` | `AlarmProcessor.aoReceber` | envia `vp` em vez de `vp.copy()` e só o converte em texto fora da secção sincronizada, após 50 ms (a pausa torna a condição de corrida observável) |

A opção vem antes de `-cp`, por exemplo
`java -Dfalha=sem-merge -cp rep03-lider/out rep03.PartitionLeader 0 3`. A falha do produtor com
N = 4 não precisa da opção.

**Diário de escritas das réplicas.** Além de `replica-<id>-p<k>.txt`, cada réplica escreve
`replica-<id>-escritas.txt`: uma linha por escrita (`instante em ms;p<k>;via;registo`), pela ordem real
em que escreveu os registos de todas as suas partições. A `via` é `pubsub`, `retido` ou `tcp`. É este
ficheiro que permite comparar os instantes de escrita no ensaio 7(e), e o `CausalCheck` analisa-o na
sua secção 8.

## Ponto 7 — Ensaios

### 7(a) — Reprodução da Figura 1

- Procedimento: no produtor, `S02 29.3`, `S01 21.0` e `S04 22.5`, uma leitura de cada vez.
- Resultado: os quatro carimbos coincidem com os da figura.

| Evento | Registo | Leitura | Figura 1 | Obtido (linha `EVENTO` do líder) |
|---|---|---|---|---|
| *a* | p1#1 | S02 29.3 | seq=1 · L=1 · V=(0,1,0) | `seq=1 L=1 V=0,1,0` |
| *b* | p0#1 | S01 21.0 | seq=1 · L=1 · V=(1,0,0) | `seq=1 L=1 V=1,0,0` |
| *c* | p2#1 | ALARM-S02 29.3 | seq=1 · L=2 · V=(0,1,1) | `seq=1 L=2 V=0,1,1` |
| *d* | p0#2 | S04 22.5 | seq=2 · L=2 · V=(2,0,0) | `seq=2 L=2 V=2,0,0` |

O processador assinalou `ALARME: S02 p1#1 → p2 (L=1 V=0,1,0)`. Relações calculadas pelo `CausalCheck`:

| Par | *V* | *L* | Relação causal | Ordem por (*L*, *k*) |
|---|---|---|---|---|
| *a*, *c* | (0,1,0) < (0,1,1) | 1 < 2 | *a* → *c* | *a* antes de *c* |
| *b*, *d* | (1,0,0) < (2,0,0) | 1 < 2 | *b* → *d* | *b* antes de *d* |
| *b*, *c* | incomparáveis | 1 < 2 | *b* ∥ *c* | *b* antes de *c* |
| *c*, *d* | incomparáveis | 2 = 2 | *c* ∥ *d* | *d* antes de *c* |
| *a*, *b* | incomparáveis | 1 = 1 | *a* ∥ *b* | *b* antes de *a* |
| *a*, *d* | incomparáveis | 1 < 2 | *a* ∥ *d* | *a* antes de *d* |

- Conclusão: as quatro primeiras linhas coincidem com a tabela do enunciado. *c* só é causalmente
  posterior a *a*, porque o processador transportou para p2 o vetor de *a*. Em (*b*, *c*) o relógio de
  Lamport ordena dois eventos concorrentes. Em (*c*, *d*) e (*a*, *b*) há empate em *L* e a ordem
  total é decidida pelo número da partição, um critério sem significado causal.

### 7(b) — `rajada 300 10` com o processador parado

| Registos p0 / p1 / p2 | Pares entre partições | Causalmente relacionados | Concorrentes | Concorrentes com *L* distinto | Violações da condição de relógio |
|---|---|---|---|---|---|
| 124 / 93 / 83 | 29 543 | **0** | 29 543 | 29 284 (99,1 %) | 0 |

- Conclusão: sem processador, nenhum par de partições diferentes é causalmente relacionado. O
  produtor envia sempre *V* = (0,0,0) e os líderes não comunicam entre si. Por isso, cada registo de
  p*k* só tem a componente *k* diferente de zero (por exemplo, `1;S07;…;1;1,0,0` e
  `2;S10;…;2;2,0,0` em p0), e quaisquer dois vetores de partições diferentes são incomparáveis.
- Mesmo assim, o relógio de Lamport ordena 99,1 % destes pares: é uma ordem sem significado causal.
  Os restantes 259 pares (0,9 %) empatam em *L*. Sem alarmes, cada líder tem *L* = `seq`, por isso os
  empates são exatamente os pares com o mesmo `seq` em partições diferentes: 93 + 83 + 83 = 259.

### 7(c) — `rajada 300 10` com o processador, três execuções

| Execução | Registos p0 / p1 / p2 | Alarmes publicados | Pares causais | Pares concorrentes | Concorrentes com *L* distinto | Violações da condição de relógio |
|---|---|---|---|---|---|---|
| 1 | 121 / 106 / 109 | 36 | 32 433 (86,3 %) | 5 136 | 4 842 (94,3 %) | 0 |
| 2 | 154 / 93 / 104 | 51 | 35 318 (88,3 %) | 4 692 | 4 435 (94,5 %) | 0 |
| 3 | 128 / 108 / 103 | 39 | 33 023 (86,6 %) | 5 109 | 4 814 (94,2 %) | 0 |

- Os alarmes publicados pelo processador coincidem com os registos `ALARM-` nos ficheiros dos
  líderes (36, 51 e 39).
- **Causalidade:** com 36 a 51 alarmes por execução, 86–88 % dos pares entre partições passam a ser
  causalmente relacionados. Cada alarme transporta o máximo de **todos** os registos que o processador
  já tinha recebido, e não só o da leitura que o originou. Por isso, liga à partição de destino tudo o
  que o processador já tinha visto nas três partições.
- **Condição de relógio:** verificou-se em todos os 100 774 pares causais das três execuções.
- **Lamport e concorrência:** a proporção de pares concorrentes com *L* distinto foi de 94,2–94,5 %,
  contra 99,1 % no ensaio 7(b); os empates em *L* passaram de 0,9 % para 5,5–5,8 % dos pares
  concorrentes. Sem alarmes, *L* = `seq`. Com alarmes, cada alarme faz subir o *L* do líder de destino
  até ao máximo observado, aproximando os contadores das três partições.
- **Réplicas:** mesmo sem perdas simuladas, cada réplica fez entre 2 e 12 pedidos de recuperação por
  TCP em cada execução. Os registos em falta nunca chegaram mais tarde pelo PubSub (0 duplicados), ou
  seja, o PubSub perdeu-os durante a rajada; a recuperação da COMINDIR02 repô-los. Na execução 2 as
  seis réplicas ficaram iguais aos líderes. Nas outras ficaram por recuperar os últimos registos de
  uma série: é a limitação da perda no fim de uma série, já identificada na COMINDIR02.
  - **Execução 1:** as réplicas 1 e 3 ficaram com p0 até ao `seq` 115, de 121. Perdeu-se o
    `p0#116` e só ficaram retidos os 5 seguintes, abaixo de `LIMITE_RETIDOS`.
  - **Execução 3:** em p1, as réplicas 1 e 2 ficaram até ao 101, de 108 (perdeu-se o `p1#102`). Em p2,
    as réplicas 2 e 3 ficaram até ao 97, de 103 (perdeu-se o `p2#98`).

  Em cada caso faltou o mesmo registo nas duas réplicas da partição. Isso indica que a mensagem se
  perdeu no PubSub antes de chegar a qualquer subscritor.

### 7(d) — Paragem e reinício do líder da partição 2

1. `rajada 60 10`. Último registo de `lider-p2.txt` antes da paragem: seq=22, L=24, V=(19,20,22).
2. Líder p2 parado. A leitura `S03 20.0` (partição 2) foi assinalada pelo produtor como
   `PERDIDA: IN;S03;20.0;0;0,0,0 → p2 (líder inacessível em 127.0.0.1:7102 (Connection refused))`.
3. Líder p2 arrancado de novo: `ARRANQUE: seq=22 L=24 V=19,20,22`.
4. Leituras seguintes:
   - `EVENTO: seq=23 L=25 V=19,20,23 (S03 21.0)`
   - `EVENTO: seq=24 L=26 V=19,20,24 (S06 29.5)`
   - `EVENTO: seq=25 L=27 V=19,20,25 (S09 23.0)`

- `CausalCheck`: *V*[2] = `seq` em todas as linhas; *L* e *V* estritamente crescentes; 0 violações.
- Conclusão: *L* e *V* prosseguem a partir da última linha de `lider-p2.txt` (`RecordFile.last()`),
  por isso o líder reiniciado não reutiliza carimbos já atribuídos. A leitura enviada com o líder
  parado não recebeu `seq` nem carimbos e não deixou qualquer vestígio nos relógios.

### 7(e) — Alarme escrito pela réplica 1 antes do registo que o originou

- Sem perdas simuladas (diários das três execuções do ensaio 7(c)), a réplica 1 escreveu 0, 4 e 4
  alarmes antes da leitura que os originou.
- Com a réplica 1 arrancada com `-Dperda=0.2` (`rajada 300 10` seguida de `rajada 30 10`), foram
  descartados 49 REG na réplica 1, que fez 42 pedidos de recuperação. As réplicas 2 e 3, sem perdas
  simuladas, fizeram 3 pedidos cada, pelas perdas do PubSub já observadas no ensaio 7(c).

| Réplica | Registos escritos antes de uma causa sua de outra partição | Alarmes escritos antes da leitura que os originou |
|---|---|---|
| 1 (com `-Dperda=0.2`) | 120 | 8 |
| 2 | 242 | 1 |
| 3 | 11 | 0 |

- Caso encontrado na réplica 1 (instantes do diário `replica-1-escritas.txt`):
  - `p1#24 ALARM-S01 29.7`, com V=(21,24,22), foi escrito às 11:48:42.568;
  - a sua origem, `p0#21 S01 29.7`, com V=(21,20,19), só foi escrita às 11:48:42.595, depois de
    recuperada por TCP.
- Conclusão: a réplica aplica cada partição de forma independente, só pela ordem do `seq`, sem
  consultar os carimbos. Um registo de p0 atrasado (retido ou recuperado por TCP) pode ser escrito
  depois de um alarme de p1 que dele depende. Os vetores permitem detetar a situação a posteriori,
  mas impedi-la exigiria entrega causal na réplica, que está fora do âmbito desta ficha.

## Ponto 8 — Falhas introduzidas

Cada falha foi introduzida isoladamente, com o código correto nos restantes processos, através de
`-Dfalha=<nome>` (sem a opção, o código é o correto; ver «Decisões de implementação») e com
`rajada 300 10`, exceto onde indicado.

| Falha introduzida | Lado onde surgiu | Detetada por `CausalCheck`? | O que permitiu concluir |
|---|---|---|---|
| Líder que incrementa *V*[*k*] sem fazer `merge` com *V*<sub>msg</sub> (`-Dfalha=sem-merge` nos três líderes) | Líder: os vetores registados perdem o conhecimento transportado pelos `IN` do processador (ex.: `p1#16 ALARM-S01` com V=(0,16,0)) | **Sim, indiretamente.** *V*[*k*] = `seq` continua válido e não há violações da condição de relógio. Mas há 0 pares causais apesar de 39 alarmes, e 39 de 39 alarmes ficam sem origem causal identificada | Sem `merge`, o vetor reduz-se ao número de sequência da partição (como na COMINDIR02) e deixa de representar a causalidade entre partições. A verificação local `V[k] = seq` não chega para detetar a falha. |
| Líder que atualiza *L* sem considerar *L*<sub>msg</sub> (`-Dfalha=lamport-local` nos três líderes) | Líder: o *L* atribuído ignora o *L* transportado pelo alarme | **Sim.** 350 violações da condição de relógio em 30 499 pares causais. Ex.: `p1#33 S02 20.7 → p0#33 ALARM-S06 29.2`, com L=33 ≥ L=33 e V (27,33,30) < (33,37,37): o alarme ficou com um *L* que não é maior do que o de um registo que o precede causalmente | A condição de relógio só é garantida pela regra de receção `max(L, L_msg) + 1`. Como *V* continua correto, o `CausalCheck` consegue confrontar as duas informações e detetar a inconsistência. |
| Líder reiniciado com *L* e *V* a zero (`rajada 100 10`, líder p2 reiniciado com `-Dfalha=reinicio-zero`, `rajada 100 10`) | Líder p2: `ARRANQUE: seq=29 L=0 V=0,0,0` e 34 linhas `VIOLAÇÃO: V[2]=… != seq=…` (a primeira com V[2]=1 e seq=30) | **Sim.** Em `lider-p2.txt`, *V*[2] = `seq` falha e *L* e *V* deixam de crescer: `p2#29`, com L=37 e V=(22,26,29), não precede `p2#30`, com L=1 e V=(0,0,1) | Um líder reiniciado tem de retomar *L* e *V* do último registo. Partindo de zero, reutiliza carimbos já atribuídos e um evento posterior fica com carimbos menores do que os anteriores. A própria verificação `V[k] == seq` do líder deteta o problema logo no primeiro evento. |
| Processador que publica o vetor sem `copy()`, alterado depois por outra *thread* (`-Dfalha=sem-copy`) | Processador: a mensagem `IN` do alarme leva o *L*<sub>p</sub> do momento da decisão e um *V*<sub>p</sub> já alterado por outra *thread* | **Sim.** 56 violações da condição de relógio em 21 763 pares causais. Ex.: `p0#58 S04 → p2#58 ALARM-S08`, com L=58 ≥ L=58 e V (58,48,21) < (61,51,58) | *L* e *V* do alarme têm de ser uma fotografia coerente tirada na mesma secção sincronizada. Sem `copy()`, *V* inclui registos recebidos depois, que *L* não reflete. É uma condição de corrida, e o número de violações varia de execução para execução. |
| Produtor executado com N = 4 e restantes processos com N = 3 (`S01 20.0`, `S02 29.0`, `rajada 30 10`) | Líderes: 29 `REJEITADO: IN;S01;20.0;0;0,0,0,0 (Vetor com dimensão 4, esperado 3 …)`. Produtor: 3 `PERDIDA` para p3, que não tem líder | **Não.** Nenhum registo foi escrito e os ficheiros ficam consistentes | *N* faz parte do formato das mensagens (dimensão do vetor) e todos os processos têm de o partilhar. A validação no líder impede misturar vetores de dimensões diferentes. Alterar *N* exige parar o sistema e converter os vetores já escritos. |

## Ponto 9 — Comparação (médias das três execuções do ensaio 7(c))

| Métrica | Valor |
|---|---|
| Registos por partição (p0 / p1 / p2) | 134,3 / 102,3 / 105,3 |
| Alarmes publicados | 42,0 |
| Pares entre partições causalmente relacionados | 33 591,3 (87,1 %) |
| Pares entre partições concorrentes | 4 979,0 (12,9 %) |
| Pares concorrentes com *L* distinto (proporção) | 4 697,0 (94,3 %) |
| Tamanho médio de uma mensagem `REG` sem carimbos (bytes) | 50,8 |
| Tamanho médio de uma mensagem `REG` com carimbos (bytes) | 62,8 |

- Os carimbos acrescentam em média 12,0 bytes por mensagem `REG` (+23,6 %): 8,8 bytes do vetor
  (`;v0,v1,v2`) e 3,1 bytes do relógio de Lamport (`;L`). Os tamanhos foram medidos sobre os
  registos escritos, com o endereço anunciado `127.0.0.1` e o porto `700<k>`.
- Os registos por partição incluem os alarmes: as 300 leituras do produtor mais 42 alarmes em média.

## Ponto 10 — Reflexão crítica

**Lamport e concorrência.** O relógio de Lamport ordenou 94,3 % dos pares concorrentes no ensaio 7(c)
e 99,1 % no 7(b). Concluir a partir de `vista-lamport.txt` que um alarme foi provocado por uma leitura
seria errado: quase todas as leituras concorrentes com o alarme aparecem antes dele nessa vista, sem
terem qualquer relação causal com ele. Na Figura 1, *b* aparece antes de *c* e não o causou. A
condição de relógio só garante que *a* → *b* ⇒ L(*a*) < L(*b*), e não o inverso. Apenas *V*(leitura) <
*V*(alarme) mostra que a leitura podia ter influenciado o alarme. Mesmo assim, isso indica
causalidade potencial: a origem concreta identifica-se pelo sensor e pela temperatura (secção 5 do
`CausalCheck`).

**Dimensão do vetor.** Com N = 3, o vetor custou em média 8,8 bytes por mensagem `REG`, a que se
somam 3,1 bytes do *L*. No total são 12,0 bytes, ou seja, +23,6 % sobre os 50,8 bytes da mensagem sem
carimbos. O mesmo vetor viaja também em cada `IN`. O custo cresce linearmente com N: uma componente por
partição, cada uma com tantos dígitos quantos os do contador mais um separador. Se N puder variar com o
sistema em funcionamento, os vetores já escritos e os que estão em trânsito ficam com uma dimensão
diferente e deixam de ser comparáveis (`VectorClock.parse` rejeita-os, como no ensaio 8 com o produtor
a N = 4). Além disso, a própria função de partição muda e os sensores passam para outras partições,
pelo que *V*[*k*] deixa de corresponder ao `seq` da partição *k*. Vetores de dimensão variável e
reparticionamento estão fora do âmbito: a solução desta ficha obriga a parar o sistema para alterar N.

**Relógios e números de sequência.** O `seq` não se torna redundante. *V*[*k*] tem o mesmo valor, mas
o `seq` continua a ser a ordem total da partição usada pela regra da COMINDIR02:
- aplicação pela ordem nas réplicas, deteção de lacunas e de duplicados;
- pedidos de recuperação `PEDIDO <de> <até>`.

O vetor acrescenta as componentes *V*[*j*], *j* ≠ *k*: quantos eventos de cada outra partição já tinham
acontecido antes deste registo, ou seja, as dependências entre partições. A redundância entre *V*[*k*] e
`seq` é aproveitada como verificação de integridade: o líder confirma `V[k] == seq` em cada evento, e
foi isso que detetou o reinício com *L* e *V* a zero no ensaio 8.

**Entrega causal.** Antes de escrever um registo *r* de p1, a réplica 1 teria de confirmar que já
escreveu em p0 todos os registos até `seq` = *V*(*r*)[0]. Ou seja, o último `seq` escrito de p0 teria de
ser ≥ *V*(*r*)[0]; caso contrário, *r* ficaria retido até p0 o alcançar. No caso do ensaio 7(e),
`p1#24 ALARM-S01` tem V=(21,24,22): só poderia ser escrito depois de `p0#21`, e a réplica 1
escreveu-o 27 ms antes. A condição só pode ser avaliada sobre as partições que a réplica guarda:
- a réplica 1 nunca recebe os registos de p2, por isso não tem como saber quando *V*(*r*)[2] estaria
  satisfeito, e esperar por eles bloqueá-la-ia para sempre;
- as dependências que passam por p2 e acabam em p0 já estão refletidas em *V*(*r*)[0], porque o vetor
  é transitivo.

O mecanismo não cabe no âmbito desta ficha, que exclui explicitamente a entrega causal nas réplicas:
a solução atual deteta a violação a posteriori, mas não a impede.

**Troço produtor → líder.** Não. O produtor envia sempre *L* = 0 e *V* = (0, 0, 0), porque não
numera as suas leituras nem tem componente no vetor. Uma leitura perdida nunca chega a ser um evento
de nenhum líder: não recebe `seq` nem carimbos. Os registos seguintes continuam sem lacunas em `seq`
nem em *V*[*k*]. Nesta implementação, o produtor só sabe da perda quando a ligação TCP é recusada
(`PERDIDA` no ensaio 7(d)). Isso é conhecimento local do produtor, não informação nos relógios.
Detetar estas perdas exigiria numeração no produtor e confirmações, mecanismos fora do âmbito.
