# Registos TCP01

## Etapa 4.3 — Variantes

### Variante compatível

- Alteração: acrescentei `setName(String name)` à `Person` nos dois projetos.
- `serialVersionUID`: manteve-se `1L`.
- Resultado: cliente e servidor continuaram a comunicar.
- Conclusão: a alteração foi compatível porque o identificador explícito não mudou.

### Variante incompatível

- Alteração: alterei o `serialVersionUID` de `Person` apenas num projeto.
- Exceção: `InvalidClassException` no servidor; no cliente ocorreu `EOFException`.
- Lado onde surgiu: a causa original surgiu no servidor; o cliente observou o fecho da ligação.
- Momento: durante a desserialização, ao executar `readObject()`.
- Conclusão: A InvalidClassException é a causa original no servidor; o EOFException do cliente é apenas consequência do fecho da ligação. A desserialização rejeitou o objeto porque os `serialVersionUID` eram diferentes, resultando numa exceção `EOFException`.

| Falha introduzida | Lado onde surgiu | Exceção obtida | Momento (ligação, escrita ou leitura) | O que a exceção permitiu (ou não) concluir |
|---|---|---|---|---|
| Arrancar o cliente sem o servidor | Cliente|IO: Connection refused: connect| Ligação|Permite concluir que o cliente tentou estabelecer a ligação TCP, mas não havia nenhum servidor a aceitar ligações no endereço/porto |
| Retirar `implements Serializable` do `Place` |Ambos|Cliente: IOException:tcp01.Place Servidor: IOException: writing aborted; java.io.NotSerializableException: tcp01.Place| escrita no cliente / leitura no servidor|O cliente não conseguiu serializar o grafo completo da Person porque Place não implementava Serializable. O servidor recebeu uma transmissão incompleta e abortou a leitura. A mensagem do servidor explica melhor a causa original. |
| `serialVersionUID` da `Person` diferente nos dois lados | Ambos| `InvalidClassException` no servidor; no cliente ocorreu `EOFException`| Leitura|A desserialização rejeitou o objeto porque os `serialVersionUID` eram diferentes, resultando numa exceção `EOFException` |
| `Person` num pacote diferente no servidor | Servidor| ClassCastException| Leitura| o TCP entregou os bytes e o servidor conseguiu desserializar um objeto identificado como tcp01.Person, mas esse objeto não pode ser convertido para outro.Person. Para a serialização Java, o nome completo da classe inclui o pacote; tcp01.Person e outro.Person são classes diferentes. 





| Construção | Onde a usou | O que ficou a ser garantido | O que continua a não ser garantido |
|---|---|---|---|
| `ServerSocket` / `accept()` |TCPServer.java | A criação de um ponto de escuta numa porta de rede e a aceitação de ligações TCP provenientes dos clientes de forma bloqueante.| A concorrência no atendimento (o accept ocorre sequencialmente) e a proteção contra interrupções abruptas de rede ou clientes indevidos.|
| `Connection extends Thread` | Connection.java / TCPServer.java| O tratamento concorrente de múltiplos clientes em simultâneo, delegando cada ligação para uma thread dedicada.|A sincronização (thread safety) em caso de acesso concorrente a recursos partilhados e a escalabilidade ilimitada (risco de esgotamento de threads se o número de clientes crescer demasiado). |
| `implements Serializable` | Person.java, Place.java| Que as instâncias destas classes podem ser convertidas numa sequência de bytes para transmissão através de fluxos de dados.| A segurança estrita contra dados corrompidos ou maliciosos durante o processo de desserialização.|
| `ObjectOutputStream` / `ObjectInputStream` | TCPClient.java, Connection.java| A serialização e desserialização transparente de objetos Java diretamente sobre os fluxos de rede. | O comportamento não bloqueante das operações de leitura/escrita e a prevenção de bloqueios (deadlocks) caso a ordem de envio/receção de mensagens falhe.|
| `serialVersionUID` | Person.java, Place.java (classes serializáveis)| Um controlo de versão consistente para evitar exceções de incompatibilidade de classes (InvalidClassException) entre o cliente e o servidor.|Que alterações estruturais profundas nos atributos ou tipos de dados sejam compatíveis em termos de lógica de negócio. |
| Referência para `Place` | Person.java | A composição de objetos e a preservação das relações estruturais (uma pessoa contém uma referência para um local) durante a serialização em grafo.|A consistência de dados distribuídos ou a sincronização do estado caso o objeto Place seja alterado noutra localização sem o devido reporte. |



  
  
  
1.15.1

Bloqueios de Fluxo (Deadlocks / Thread Starvation): Erros na sincronização ou na ordem de leitura/escrita de fluxos (ObjectInputStream / ObjectOutputStream) que deixam as threads bloqueadas indefinidamente à espera de dados que nunca chegam.

Exceções de Lógica e Validação: Falhas no tratamento interno de dados a nível de aplicação, onde o transporte físico ocorre sem erros, mas a interpretação do objeto viola as regras de negócio.


1.15.2  
Modificar a classe Person (adicionar, remover ou alterar o tipo de atributos) quebra a compatibilidade binária se o serialVersionUID não for explicitamente declarado e gerido de forma rigorosa.

Sem um identificador fixo, qualquer alteração menor altera o hash calculado automaticamente pelo compilador, impedindo a comunicação entre versões diferentes do cliente e do servidor.

A evolução exige estratégias de compatibilidade (como o uso de campos transient ou planeamento de migração de dados) para evitar falhas abruptas no sistema.

1.15.3

Crescimento excessivo da carga (Payload): Se uma classe como Person referencia Place e este contiver outras referências encadeadas, o mecanismo de serialização em grafo transporta toda a árvore de objetos, disparando o consumo de largura de banda.

Desempenho e Consumo de Memória: O processo de serialização de grafos complexos exige mais ciclos de CPU e alocação de memória, podendo gerar problemas de performance.

Acoplamento e Segurança: Expõe o estado interno integral dos objetos, dificultando o encapsulamento e aumentando a rigidez estrutural da aplicação.

1.15.4

Exaustão de Recursos (Resource Exhaustion): Cada thread consome uma quantidade fixa de memória para a sua pilha (stack). Um número elevado de clientes simultâneos pode esgotar a memória RAM ou atingir o limite de threads do sistema operativo.

Sobrecarga de Contexto (Context Switching): O processador perde tempo precioso a gerir a alternância entre centenas de threads ativas, degradando a eficiência global do servidor.

Falta de Escalabilidade: Este modelo não é adequado para cargas elevadas, sendo preferível utilizar arquiteturas baseadas em Thread Pools (ExecutorService) ou I/O assíncrono/não bloqueante (NIO).

1.15.5  
Baixa Latência Crítica: Em cenários onde a velocidade de entrega é muito mais importante do que a garantia de entrega (ex: jogos online, transmissão de áudio/vídeo em tempo real ou sensores de telemetria).

Comunicação Broadcast ou Multicast: Quando é necessário enviar a mesma mensagem para múltiplos destinatários na rede sem a necessidade de estabelecer e manter conexões ponto a ponto dedicadas.

Tolerância a Perdas: Quando a perda ocasional de pacotes de dados não afeta criticamente a integridade global do sistema, dispensando o overhead de controlo de fluxo e retransmissão inerente ao TCP.  