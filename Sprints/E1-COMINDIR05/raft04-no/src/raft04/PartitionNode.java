package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Nó de uma partição: eleição, replicação, recuperação TCP e reconciliação.
 * A rede não pode ser consultada mantendo nodeLock: a recuperação usa um executor
 * próprio e confirma termo/líder e o snapshot antes da substituição atómica.
 */
public final class PartitionNode implements AutoCloseable {
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    private final int partition;
    private final int nodeId;
    private final int partitions;
    private final String advertisedHost;
    private final IpfsPubSub pubSub;
    private final Partitioner partitioner;
    private final RecordFile file;
    private final RaftState raft;
    private final Path eventsFile;
    private final Path heartbeatsFile;
    private final Object nodeLock = new Object();
    private final Object publishLock = new Object();
    private final Object eventsLock = new Object();
    private volatile ScheduledExecutorService timers;
    private volatile ExecutorService recoveryWorker;
    private boolean recoveryScheduled; // apenas sob nodeLock
    private RecoveryTarget pendingRecovery; // sempre o alvo mais recente; sob nodeLock
    private long batchVersion; // muda quando chega um novo LOTE aceite; sob nodeLock
    private long lastAcceptedBatchTerm = -1;
    private int lastAcceptedBatchLeader;
    private long lastAcceptedBatchHb = -1;
    private long lastAcceptedBatchSeq = -1;
    private long lastAcceptedBatchLastTerm = -1;

    private record RecoveryTarget(long seq, long lastTerm, int leaderId,
                                  long electionTerm, long version) { }
    private volatile Thread inputThread;
    private volatile Thread dataThread;
    private volatile Thread electionThread;

    // Leader-side state; all mutations must be coordinated with nodeLock.
    private long lastSeq;
    private long lastTerm;
    private long lamport;
    private VectorClock vector;
    private long heartbeat;
    private List<SensorRecord> pending = new ArrayList<>();

    // Servidor TCP independente. O pool limitado evita criar threads sem limite.
    private volatile ServerSocket tcpServer;
    private volatile Thread tcpThread;
    private volatile ThreadPoolExecutor tcpWorkers;
    private volatile boolean running;

    public PartitionNode(int partition, int nodeId, int partitions,
                         String advertisedHost, String api) throws IOException {
        this(partition, nodeId, partitions, advertisedHost, new IpfsPubSub(api));
    }

    // Permite testar o protocolo sem um daemon IPFS, injetando transporte falso.
    PartitionNode(int partition, int nodeId, int partitions,
                  String advertisedHost, IpfsPubSub transport) throws IOException {
        if (partitions != Wire.GRUPO) {
            throw new IllegalArgumentException("Esta ficha requer N=3 partições");
        }
        if (partition < 0 || partition >= partitions) {
            throw new IllegalArgumentException("Partição fora do intervalo [0, N)");
        }
        if (nodeId < 1 || nodeId > Wire.GRUPO) {
            throw new IllegalArgumentException("ID do nó fora do intervalo [1, G]");
        }
        if (advertisedHost == null || advertisedHost.isBlank()
                || advertisedHost.indexOf(';') >= 0) {
            throw new IllegalArgumentException("Endereço anunciado inválido");
        }

        this.partition = partition;
        this.nodeId = nodeId;
        this.partitions = partitions;
        this.advertisedHost = advertisedHost;
        this.pubSub = java.util.Objects.requireNonNull(transport, "transporte IPFS");
        this.partitioner = new Partitioner(partitions);
        this.file = new RecordFile("no-p" + partition + "-n" + nodeId + ".txt");
        this.raft = new RaftState(
                Path.of("estado-p" + partition + "-n" + nodeId + ".txt"), nodeId);
        this.eventsFile = Path.of("eventos-p" + partition + "-n" + nodeId + ".txt");
        this.heartbeatsFile = Path.of("hb-p" + partition + "-n" + nodeId + ".txt");
        this.vector = new VectorClock(partitions);
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: PartitionNode <k> <i> [N]");
            return;
        }
        try {
            int partition = Integer.parseInt(args[0]);
            int nodeId = Integer.parseInt(args[1]);
            int partitions = args.length == 3 ? Integer.parseInt(args[2]) : Wire.GRUPO;
            PartitionNode node = new PartitionNode(partition, nodeId, partitions,
                    "127.0.0.1", DEFAULT_API);
            node.run();
        } catch (NumberFormatException e) {
            System.err.println("k, i e N têm de ser inteiros.");
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("Não foi possível iniciar o nó: " + e.getMessage());
        }
    }

    private void run() throws IOException {
        start();
        try {
            tcpThread.join(); // main mantém o nó ativo até encerrar.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    /** Inicia os três subscritores, o servidor TCP e as tarefas periódicas. */
    public synchronized void start() throws IOException {
        if (running) throw new IllegalStateException("Nó já iniciado");
        loadLocalState();
        startRecoveryServer();
        // Regista o arranque bem-sucedido da instância, antes dos subscritores
        // e dos temporizadores; RaftCheck usa o primeiro ARRANQUE global.
        logEvent("ARRANQUE;n=" + nodeId);
        recoveryWorker = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "raft04-recuperacao-p" + partition + "-n" + nodeId);
            thread.setDaemon(true);
            return thread;
        });
        inputThread = subscriberThread("in", this::subscribeInput);
        dataThread = subscriberThread("dados", this::subscribeData);
        electionThread = subscriberThread("eleicao", this::subscribeElection);
        inputThread.start();
        dataThread.start();
        electionThread.start();

        ThreadFactory factory = action -> {
            Thread thread = new Thread(action, "raft04-agenda-p" + partition + "-n" + nodeId);
            thread.setDaemon(true);
            return thread;
        };
        timers = Executors.newScheduledThreadPool(2, factory);
        timers.scheduleWithFixedDelay(() -> {
            try { checkElection(); }
            catch (RuntimeException e) { logEvent("ERRO_ELEICAO;" + e.getMessage()); }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        timers.scheduleWithFixedDelay(() -> {
            try { sendBatch(); }
            catch (RuntimeException e) { logEvent("ERRO_LOTE;" + e.getMessage()); }
        }, Wire.PERIODO_HB_MS, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private Thread subscriberThread(String suffix, Runnable task) {
        Thread thread = new Thread(task,
                "raft04-" + suffix + "-p" + partition + "-n" + nodeId);
        thread.setDaemon(true);
        return thread;
    }

    /** Recupera os dados do último registo; rejeita logs antigos ou inválidos. */
    private void loadLocalState() throws IOException {
        List<SensorRecord> records;
        try {
            records = file.readAll();
        } catch (IOException e) {
            throw new IOException("Não foi possível recuperar os registos do nó p"
                    + partition + "-n" + nodeId
                    + ". Confirma que não é um ficheiro COMINDIR04 antigo de 6 campos.", e);
        }
        for (SensorRecord record : records) {
            VectorClock v = record.getVector();
            if (v.size() != partitions || v.get(partition) != record.getSeq()) {
                throw new IOException("Registo seq=" + record.getSeq()
                        + " viola a dimensão do vetor ou V[" + partition + "] = seq");
            }
        }
        synchronized (nodeLock) {
            if (records.isEmpty()) {
                lastSeq = 0;
                lastTerm = 0;
                lamport = 0;
                vector = new VectorClock(partitions);
            } else {
                SensorRecord last = records.get(records.size() - 1);
                lastSeq = last.getSeq();
                lastTerm = last.getTerm();
                lamport = last.getLamport();
                vector = last.getVector();
            }
        }
        System.out.println("NÓ p" + partition + "-n" + nodeId
                + " recuperado: seq=" + lastSeq + ", termoRegisto=" + lastTerm
                + ", L=" + lamport + ", V=" + vector
                + ", termoEleitoral=" + raft.getTerm());
    }

    /** Abre a porta antes de criar a thread, para reportar erros de bind. */
    private synchronized void startRecoveryServer() throws IOException {
        if (running) {
            throw new IllegalStateException("Servidor TCP já iniciado");
        }
        ServerSocket server = new ServerSocket();
        try {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(Wire.nodePort(partition, nodeId)));
            server.setSoTimeout(500);
        } catch (IOException e) {
            server.close();
            throw e;
        }
        tcpServer = server;
        tcpWorkers = new ThreadPoolExecutor(
                2, 4, 30L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(16),
                task -> {
                    Thread thread = new Thread(task,
                            "raft04-tcp-worker-p" + partition + "-n" + nodeId);
                    thread.setDaemon(true);
                    return thread;
                });
        running = true;
        tcpThread = new Thread(this::acceptRecoveryConnections,
                "raft04-tcp-p" + partition + "-n" + nodeId);
        tcpThread.start();
        System.out.println("TCP recuperação em " + Wire.nodePort(partition, nodeId));
    }

    private void acceptRecoveryConnections() {
        while (running) {
            try {
                Socket socket = tcpServer.accept();
                if (!running || Wire.isolated(partition, nodeId)) {
                    socket.close();
                    continue;
                }
                try {
                    tcpWorkers.execute(() -> {
                        try (socket) {
                            socket.setSoTimeout(Wire.T_TCP_MS);
                            serveRecovery(socket);
                        } catch (IOException e) {
                            if (running && !Wire.isolated(partition, nodeId)) {
                                System.err.println("Recuperação TCP: " + e.getMessage());
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    socket.close();
                }
            } catch (SocketTimeoutException ignored) {
                // Verifica periodicamente se o servidor foi desligado.
            } catch (SocketException e) {
                if (running) System.err.println("Falha TCP: " + e.getMessage());
                break;
            } catch (IOException e) {
                if (running) System.err.println("Falha TCP: " + e.getMessage());
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        running = false;
        if (timers != null) timers.shutdownNow();
        if (recoveryWorker != null) recoveryWorker.shutdownNow();
        if (inputThread != null) inputThread.interrupt();
        if (dataThread != null) dataThread.interrupt();
        if (electionThread != null) electionThread.interrupt();
        if (tcpServer != null) tcpServer.close();
        if (tcpWorkers != null) tcpWorkers.shutdownNow();
    }

    private void subscribeInput() {
        subscribeLoop(Wire.topicIn(partition), this::onInput);
    }

    private void subscribeData() {
        subscribeLoop(Wire.topicData(partition), message -> {
            if (!Wire.isolated(partition, nodeId)) onBatch(message);
        });
    }

    private void subscribeElection() {
        subscribeLoop(Wire.topicElection(partition), message -> {
            if (!Wire.isolated(partition, nodeId)) onElectionMessage(message);
        });
    }

    private void subscribeLoop(String topic, java.util.function.Consumer<String> handler) {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                pubSub.subscribe(topic, message -> {
                    try { handler.accept(message); }
                    catch (RuntimeException e) {
                        logEvent("MENSAGEM_REJEITADA;" + topic + ";" + e.getMessage());
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException | RuntimeException e) {
                if (running) {
                    logEvent("SUBSCRICAO_FALHOU;" + topic + ";" + e.getMessage());
                }
            }
            if (running) {
                try { Thread.sleep(1000L); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** IN;sensor;temperatura;L;V: só o líder cria registos. */
    private void onInput(String message) {
        try {
            if (!"IN".equals(Wire.type(message))) return;
            String[] fields = Wire.body(message).split(";", -1);
            if (fields.length != 4 || fields[0].isBlank()
                    || partitioner.of(fields[0]) != partition) {
                throw new IllegalArgumentException("IN inválido ou da partição errada");
            }
            double temperature = Double.parseDouble(fields[1]);
            long incomingLamport = Long.parseLong(fields[2]);
            VectorClock incomingVector = VectorClock.parse(fields[3], partitions);
            if (!Double.isFinite(temperature) || incomingLamport < 0) {
                throw new IllegalArgumentException("Temperatura ou relógio inválido");
            }
            for (int k = 0; k < partitions; k++) {
                if (incomingVector.get(k) < 0) {
                    throw new IllegalArgumentException("Componente negativa do vetor");
                }
            }
            SensorRecord record;
            synchronized (nodeLock) {
                if (raft.getRole() != RaftState.Role.LIDER) {
                    logEvent("IGNORADO;IN sem liderança;sensor=" + fields[0]);
                    return;
                }
                long currentTerm = raft.getTerm();
                long nextSeq = Math.addExact(lastSeq, 1L);
                long nextLamport = Math.addExact(Math.max(lamport, incomingLamport), 1L);
                VectorClock nextVector = vector.copy();
                nextVector.merge(incomingVector);
                nextVector.tick(partition);
                if (nextVector.get(partition) != nextSeq) {
                    throw new IllegalArgumentException("IN viola V[k]=seq");
                }
                record = SensorRecord.now(nextSeq, currentTerm, fields[0],
                        temperature, nextLamport, nextVector);
                file.append(record); // Primeiro o disco; depois a memória.
                lastSeq = nextSeq;
                lastTerm = currentTerm;
                lamport = nextLamport;
                vector = nextVector;
                pending.add(record);
            }
            logEvent("IN_APLICADO;" + record.getSeq() + ";" + record.getTerm());
        } catch (IllegalArgumentException | ArithmeticException e) {
            logEvent("IN_REJEITADO;" + e.getMessage());
        } catch (IOException e) {
            logEvent("IN_ERRO_IO;" + e.getMessage());
        }
    }

    /** Aplica um prefixo contíguo e recupera, fora do callback, falhas e divergências. */
    private void onBatch(String message) {
        if (Wire.isolated(partition, nodeId)) return;
        try {
            Batch batch = Batch.decode(message, partitions);
            if (batch.getPort() != Wire.nodePort(partition, batch.getLeader())) {
                throw new IllegalArgumentException("Porta do líder não corresponde ao nó");
            }
            for (SensorRecord record : batch.getRecords()) {
                if (record.getVector().get(partition) != record.getSeq()) {
                    throw new IllegalArgumentException("Registo viola V[k]=seq");
                }
            }
            if (batch.getLeader() == nodeId && raft.getRole() == RaftState.Role.LIDER
                    && batch.getTerm() == raft.getTerm()) return;
            boolean recoveryNeeded = false;
            boolean accepted;
            long acceptedVersion;
            synchronized (nodeLock) {
                // LOTE antigo do mesmo líder/termo não pode provocar rollback.
                if (batch.getTerm() == lastAcceptedBatchTerm
                        && batch.getLeader() == lastAcceptedBatchLeader
                        && batch.getHb() <= lastAcceptedBatchHb) return;
                long previousTerm = raft.getTerm();
                RaftState.Role previousRole = raft.getRole();
                int previousLeader = raft.getKnownLeader();
                accepted = raft.onBatch(batch.getTerm(), batch.getLeader());
                logRaftTransitions(previousTerm, previousRole, previousLeader, "LOTE");
                if (!accepted) {
                    logEvent("IGNORADO;termo=" + batch.getTerm()
                            + ";lider=n" + batch.getLeader());
                    return;
                }
                // Não cancelar uma recuperação lenta só porque chegou outro heartbeat
                // com o MESMO alvo: caso contrário pode nunca terminar.
                boolean targetChanged = batch.getTerm() != lastAcceptedBatchTerm
                        || batch.getLeader() != lastAcceptedBatchLeader
                        || batch.getLastSeq() != lastAcceptedBatchSeq
                        || batch.getLastTerm() != lastAcceptedBatchLastTerm;
                lastAcceptedBatchTerm = batch.getTerm();
                lastAcceptedBatchLeader = batch.getLeader();
                lastAcceptedBatchHb = batch.getHb();
                lastAcceptedBatchSeq = batch.getLastSeq();
                lastAcceptedBatchLastTerm = batch.getLastTerm();
                // Só invalida uma recuperação se mudou a versão autoritativa.
                if (targetChanged) ++batchVersion;
                acceptedVersion = batchVersion;
                if (raft.getRole() == RaftState.Role.SEGUIDOR) pending.clear();
                for (SensorRecord record : batch.getRecords()) {
                    if (record.getSeq() <= lastSeq) {
                        List<SensorRecord> current = file.range(record.getSeq(), record.getSeq());
                        if (current.size() != 1 || !sameRecord(current.get(0), record)) {
                            recoveryNeeded = true;
                            break;
                        }
                    } else if (record.getSeq() == lastSeq + 1 && !recoveryNeeded) {
                        file.append(record);
                        updateLastRecord(record);
                    } else {
                        recoveryNeeded = true;
                        break;
                    }
                }
                if (lastSeq != batch.getLastSeq() || lastTerm != batch.getLastTerm()) {
                    recoveryNeeded = true;
                }
            }
            logEvent("RECEBIDO;termo=" + batch.getTerm() + ";lider=n"
                    + batch.getLeader() + ";hb=" + batch.getHb()
                    + ";ultimoSeq=" + batch.getLastSeq());
            if (recoveryNeeded) {
                scheduleRecovery(batch.getLastSeq(), batch.getLastTerm(),
                        batch.getLeader(), batch.getTerm(), acceptedVersion);
            }
        } catch (IllegalArgumentException e) {
            logEvent("IGNORADO;LOTE inválido: " + e.getMessage());
        } catch (IOException e) {
            logEvent("ERRO;LOTE IO: " + e.getMessage());
        }
    }

    private static boolean sameRecord(SensorRecord left, SensorRecord right) {
        // Na ficha, a identidade da entrada é exclusivamente (seq, termo).
        return left.getSeq() == right.getSeq() && left.getTerm() == right.getTerm();
    }

    /** Deve ser chamado sob nodeLock. */
    private void updateLastRecord(SensorRecord record) {
        lastSeq = record.getSeq();
        lastTerm = record.getTerm();
        lamport = record.getLamport();
        vector = record.getVector();
    }

    /** Substitui o alvo pendente pelo LOTE mais recente, sem criar tarefas ilimitadas. */
    private void scheduleRecovery(long leaderSeq, long leaderLastTerm, int leaderId,
                                  long leaderElectionTerm, long version) {
        synchronized (nodeLock) {
            if (!running || Wire.isolated(partition, nodeId) || version != batchVersion) {
                return;
            }
            pendingRecovery = new RecoveryTarget(leaderSeq, leaderLastTerm,
                    leaderId, leaderElectionTerm, version);
            if (recoveryScheduled) return; // o worker existente irá usar o alvo mais recente
            recoveryScheduled = true;
        }
        try {
            recoveryWorker.execute(this::processRecoveries);
        } catch (RejectedExecutionException e) {
            synchronized (nodeLock) {
                pendingRecovery = null;
                recoveryScheduled = false;
            }
            logEvent("ERRO;Executor de recuperação indisponível");
        }
    }

    /** Um único worker, sem I/O TCP enquanto mantém nodeLock. */
    private void processRecoveries() {
        while (true) {
            RecoveryTarget target;
            synchronized (nodeLock) {
                target = pendingRecovery;
                pendingRecovery = null;
                if (!running || target == null) {
                    recoveryScheduled = false;
                    return;
                }
            }
            try {
                reconcileWithLeader(target.seq(), target.lastTerm(), target.leaderId(),
                        target.electionTerm(), target.version());
            } catch (IOException | RuntimeException e) {
                logEvent("ERRO;RECUPERACAO: " + e.getMessage());
            }
        }
    }

    /** Encaminha pedidos e respostas de voto para RaftState. */
    private void onElectionMessage(String message) {
        if (Wire.isolated(partition, nodeId)) return;
        try {
            switch (Wire.type(message)) {
                case "PEDIDO_VOTO" -> {
                    VoteRequest request = VoteRequest.decode(message);
                    if (request.candidate == nodeId) return;
                    RaftState.Vote result;
                    synchronized (nodeLock) {
                        long prevTerm = raft.getTerm();
                        RaftState.Role prevRole = raft.getRole();
                        int prevLeader = raft.getKnownLeader();
                        result = raft.onVoteRequest(request.term,
                                request.candidate, request.lastSeq, request.lastTerm,
                                lastSeq, lastTerm);
                        logRaftTransitions(prevTerm, prevRole, prevLeader, "PEDIDO_VOTO");
                    }
                    VoteReply reply = new VoteReply(result.term, nodeId,
                            request.candidate, result.granted);
                    logEvent("VOTO;eleitor=n" + nodeId + ";candidato=n" + request.candidate
                            + ";termo=" + result.term + ";"
                            + (result.granted ? "concedido" : "recusado"));
                    publishElection(reply.encode());
                }
                case "VOTO" -> {
                    VoteReply reply = VoteReply.decode(message);
                    if (reply.candidate != nodeId || reply.voter == nodeId) return;
                    boolean elected;
                    synchronized (nodeLock) {
                        long prevTerm = raft.getTerm();
                        RaftState.Role prevRole = raft.getRole();
                        int prevLeader = raft.getKnownLeader();
                        elected = raft.onVote(reply.term, reply.voter,
                                reply.candidate, reply.granted);
                        logRaftTransitions(prevTerm, prevRole, prevLeader, "VOTO");
                    }
                    if (elected) becomeLeader();
                }
                default -> { /* tópico de eleição pode receber tráfego não reconhecido */ }
            }
        } catch (IllegalArgumentException e) {
            logEvent("ELEICAO_REJEITADA;" + e.getMessage());
        } catch (IOException e) {
            logEvent("ELEICAO_ERRO_IO;" + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void publishElection(String message) throws IOException, InterruptedException {
        if (running && !Wire.isolated(partition, nodeId)) {
            pubSub.publish(Wire.topicElection(partition), message);
        }
    }

    /** Elege um candidato após expirar o prazo, sem enviar mensagens no isolamento. */
    private void checkElection() {
        if (!running || Wire.isolated(partition, nodeId)) return;
        try {
            long candidateTerm, localSeq, localTerm, previousTerm;
            synchronized (nodeLock) {
                previousTerm = raft.getTerm();
                candidateTerm = raft.tick();
                if (candidateTerm < 0) return;
                localSeq = lastSeq;
                localTerm = lastTerm;
            }
            VoteRequest request = new VoteRequest(candidateTerm, nodeId, localSeq, localTerm);
            logEvent("TERMO;de=" + previousTerm + ";para=" + candidateTerm
                    + ";origem=CANDIDATURA");
            logEvent("CANDIDATO;termo=" + candidateTerm + ";seq=" + localSeq);
            if (raft.getRole() == RaftState.Role.CANDIDATO
                    && raft.getTerm() == candidateTerm) {
                publishElection(request.encode());
            }
        } catch (IOException e) {
            logEvent("ELEICAO_ERRO_IO;" + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void becomeLeader() {
        if (Wire.isolated(partition, nodeId) || !running
                || raft.getRole() != RaftState.Role.LIDER) return;
        synchronized (nodeLock) {
            if (raft.getRole() != RaftState.Role.LIDER) return;
            // seq, L, V e ultimoTermo já vêm do registo local do Bloco 1.
            // A nova liderança não pode repor esses valores a zero.
            heartbeat = 0;
            pending.clear();
        }
        logEvent("LIDER;termo=" + raft.getTerm() + ";n=" + nodeId);
        sendBatch(); // primeiro heartbeat sem esperar pelo período seguinte
    }

    /** Envia LOTE periódico mesmo quando não há novos registos. */
    private void sendBatch() {
        if (!running || Wire.isolated(partition, nodeId)) return;
        synchronized (publishLock) {
            if (raft.getRole() != RaftState.Role.LIDER || Wire.isolated(partition, nodeId)) {
                return;
            }
            Batch batch;
            List<SensorRecord> snapshot;
            long sentTerm;
            synchronized (nodeLock) {
                sentTerm = raft.getTerm();
                snapshot = new ArrayList<>(pending);
                pending.clear();
                batch = new Batch(sentTerm, nodeId, advertisedHost,
                        Wire.nodePort(partition, nodeId), ++heartbeat,
                        lastSeq, lastTerm, snapshot);
            }
            try {
                if (raft.getRole() == RaftState.Role.LIDER && raft.getTerm() == sentTerm
                        && !Wire.isolated(partition, nodeId)) {
                    String payload = batch.encode();
                    pubSub.publish(Wire.topicData(partition), payload);
                    writeHeartbeat(batch, payload);
                    logEvent("RECEBIDO;LOTE_PUBLICADO;termo=" + sentTerm + ";hb="
                            + batch.getHb() + ";ultimoSeq=" + batch.getLastSeq()
                            + ";n=" + snapshot.size());
                } else {
                    restorePending(snapshot, sentTerm);
                }
            } catch (IOException e) {
                restorePending(snapshot, sentTerm);
                logEvent("LOTE_ERRO_IO;" + e.getMessage());
            } catch (InterruptedException e) {
                restorePending(snapshot, sentTerm);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void restorePending(List<SensorRecord> snapshot, long sentTerm) {
        synchronized (nodeLock) {
            if (raft.getRole() == RaftState.Role.LIDER && raft.getTerm() == sentTerm) {
                pending.addAll(0, snapshot); // mantém a ordem se chegaram IN entretanto
            }
        }
    }

    /** Responde a PEDIDO <from> <to>, seja líder ou seguidor. */
    private void serveRecovery(Socket socket) throws IOException {
        if (Wire.isolated(partition, nodeId)) return;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                     socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            String request = reader.readLine();
            if (Wire.isolated(partition, nodeId)) return;
            String[] parts = request == null ? new String[0] : request.trim().split("\\s+");
            if (parts.length != 3 || !"PEDIDO".equals(parts[0])) {
                writer.println("ERRO pedido mal formado");
                return;
            }
            long from, to;
            try {
                from = Long.parseLong(parts[1]);
                to = Long.parseLong(parts[2]);
            } catch (NumberFormatException e) {
                writer.println("ERRO sequência inválida");
                return;
            }
            if (from < 1 || to < from) {
                writer.println("ERRO intervalo inválido");
                return;
            }
            List<SensorRecord> records;
            // Ficheiro e metadados de replicação são protegidos pelo mesmo lock.
            synchronized (nodeLock) {
                records = file.range(from, to);
            }
            for (SensorRecord record : records) {
                if (Wire.isolated(partition, nodeId)) return;
                writer.println(record.toLine());
            }
            if (Wire.isolated(partition, nodeId)) return;
            writer.println("FIM");
            if (writer.checkError()) {
                throw new IOException("Falha a escrever resposta de recuperação");
            }
        }
    }

    /** Cliente de recuperação TCP (as divergências serão tratadas no bloco seguinte). */
    private List<SensorRecord> requestRecovery(int peerId, long from, long to)
            throws IOException {
        if (peerId < 1 || peerId > Wire.GRUPO || from < 1 || to < from) {
            throw new IllegalArgumentException("Nó ou intervalo de recuperação inválido");
        }
        if (Wire.isolated(partition, nodeId) || Wire.isolated(partition, peerId)) {
            throw new IOException("Recuperação TCP bloqueada pelo isolamento");
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", Wire.nodePort(partition, peerId)),
                    Wire.T_TCP_MS);
            socket.setSoTimeout(Wire.T_TCP_MS);
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                         socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(
                         socket.getInputStream(), StandardCharsets.UTF_8))) {
                writer.println("PEDIDO " + from + " " + to);
                if (writer.checkError()) throw new IOException("Falha ao enviar PEDIDO");
                List<SensorRecord> response = new ArrayList<>();
                boolean finished = false;
                String line;
                while ((line = reader.readLine()) != null) {
                    if (Wire.isolated(partition, nodeId)) {
                        throw new IOException("Recuperação interrompida: nó isolado");
                    }
                    if ("FIM".equals(line)) {
                        finished = true;
                        break;
                    }
                    if (line.startsWith("ERRO ")) throw new IOException(line);
                    SensorRecord record;
                    try {
                        record = SensorRecord.fromLine(line);
                    } catch (IllegalArgumentException e) {
                        throw new IOException("Registo TCP mal formado", e);
                    }
                    if (record.getSeq() < from || record.getSeq() > to
                            || (response.isEmpty() && record.getSeq() != from)
                            || (!response.isEmpty() && record.getSeq()
                                    != response.get(response.size() - 1).getSeq() + 1)
                            || record.getVector().size() != partitions
                            || record.getVector().get(partition) != record.getSeq()) {
                        throw new IOException("Sequência inválida na resposta TCP");
                    }
                    response.add(record);
                }
                if (!finished) throw new IOException("Resposta TCP sem FIM");
                return response;
            }
        }
    }

    /**
     * Reconcilia com o log de um líder reconhecido. Parte do último bloco local de
     * termo, recuando bloco a bloco até encontrar um primeiro registo coincidente.
     * A consulta TCP ocorre sem nodeLock; o commit faz uma única escrita atómica.
     */
    private void reconcileWithLeader(long leaderSeq, long leaderLastTerm, int leaderId,
                                     long electionTerm, long version) throws IOException {
        if (leaderSeq < 0 || leaderLastTerm < 0 || leaderLastTerm > electionTerm) {
            throw new IllegalArgumentException("Metadados do líder inválidos");
        }
        List<SensorRecord> snapshot;
        synchronized (nodeLock) {
            if (version != batchVersion || !isCurrentLeader(leaderId, electionTerm)) return;
            snapshot = file.readAll();
        }
        List<SensorRecord> authoritative;
        int start = lastTermBlockStart(snapshot, snapshot.size()); // seq 1 se vazio
        if (leaderSeq == 0) {
            authoritative = List.of();
            start = 1;
        } else {
            while (true) {
                if (start > leaderSeq) {
                    start = lastTermBlockStart(snapshot, start - 1);
                    continue;
                }
                if (Wire.isolated(partition, nodeId) || Wire.isolated(partition, leaderId)) {
                    throw new IOException("Recuperação interrompida pelo isolamento");
                }
                List<SensorRecord> received = requestRecovery(leaderId, start, leaderSeq);
                if (!received.isEmpty()) {
                    SensorRecord last = received.get(received.size() - 1);
                    if (last.getSeq() != leaderSeq || last.getTerm() != leaderLastTerm) {
                        throw new IOException("Resposta TCP não corresponde ao último registo do LOTE");
                    }
                } else if (start <= leaderSeq) {
                    throw new IOException("Resposta TCP incompleta para seq " + start);
                }
                if (start == 1 || (!received.isEmpty() && start <= snapshot.size()
                        && sameRecord(received.get(0), snapshot.get(start - 1)))) {
                    authoritative = received;
                    break;
                }
                start = lastTermBlockStart(snapshot, start - 1);
            }
        }

        // Calcula o primeiro seq que não coincide. Se o líder tiver log mais curto,
        // a primeira diferença é o primeiro registo local além do fim do líder.
        List<SensorRecord> reconciled = new ArrayList<>();
        int prefix = Math.min(start - 1, snapshot.size());
        reconciled.addAll(snapshot.subList(0, prefix));
        int divergent = 0;
        for (SensorRecord remote : authoritative) {
            int index = Math.toIntExact(remote.getSeq() - 1);
            if (divergent == 0 && (index >= snapshot.size()
                    || !sameRecord(snapshot.get(index), remote))) {
                divergent = index + 1;
            }
            // O prefixo com a mesma identidade (seq,term) é preservado.
            reconciled.add(index < snapshot.size()
                    && sameRecord(snapshot.get(index), remote) ? snapshot.get(index) : remote);
        }
        if (divergent == 0 && snapshot.size() > leaderSeq) {
            divergent = Math.toIntExact(leaderSeq + 1);
        }
        // A reconstrução deve representar um prefixo exato do registo do líder.
        if (reconciled.size() != leaderSeq) {
            throw new IOException("Reconciliação não produziu o total indicado pelo líder");
        }
        for (int j = 0; j < reconciled.size(); j++) {
            SensorRecord record = reconciled.get(j);
            if (record.getSeq() != j + 1 || record.getVector().size() != partitions
                    || record.getVector().get(partition) != record.getSeq()) {
                throw new IOException("Registo reconciliado inválido em seq " + (j + 1));
            }
        }
        if (leaderSeq > 0 && reconciled.get(reconciled.size() - 1).getTerm()
                != leaderLastTerm) {
            throw new IOException("Termo do último registo reconciliado inválido");
        }
        List<SensorRecord> removed = new ArrayList<>();
        if (divergent > 0 && divergent <= snapshot.size()) {
            removed.addAll(snapshot.subList(divergent - 1, snapshot.size()));
        }
        synchronized (nodeLock) {
            // Uma mensagem mais recente invalida o alvo anterior ATÉ no commit.
            if (version != batchVersion || !isCurrentLeader(leaderId, electionTerm)) return;
            if (!sameLog(snapshot, file.readAll())) {
                throw new IOException("Registo local alterado durante recuperação; repetir no próximo LOTE");
            }
            // Não reescreve um ficheiro sem divergências; nunca trunca antes do TCP.
            if (!sameLog(snapshot, reconciled)) file.replaceAll(reconciled);
            if (reconciled.isEmpty()) {
                lastSeq = 0;
                lastTerm = 0;
                lamport = 0;
                vector = new VectorClock(partitions);
            } else {
                updateLastRecord(reconciled.get(reconciled.size() - 1));
            }
            pending.clear();
        }
        if (!removed.isEmpty()) {
            StringBuilder terms = new StringBuilder();
            for (SensorRecord record : removed) {
                if (terms.length() > 0) terms.append(',');
                terms.append(record.getTerm());
            }
            logEvent("TRUNCADO;de=" + divergent + ";descartados=" + removed.size()
                    + ";termos=" + terms);
        }
        logEvent("RECUPERACAO;lider=n" + leaderId + ";ultimoSeq=" + leaderSeq
                + ";termo=" + electionTerm + ";recebidos=" + authoritative.size());
    }

    /** Devolve o primeiro seq do bloco de termo que termina em index (1-based). */
    private static int lastTermBlockStart(List<SensorRecord> records, int index) {
        if (index <= 0) return 1;
        int at = Math.min(index, records.size());
        long blockTerm = records.get(at - 1).getTerm();
        while (at > 1 && records.get(at - 2).getTerm() == blockTerm) at--;
        return at;
    }

    private static boolean sameLog(List<SensorRecord> a, List<SensorRecord> b) {
        if (a.size() != b.size()) return false;
        // Para detetar mutações concorrentes do snapshot, exige igualdade física.
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).toLine().equals(b.get(i).toLine())) return false;
        }
        return true;
    }

    /** Deve ser chamado sob nodeLock. */
    private boolean isCurrentLeader(int leaderId, long electionTerm) {
        return running && !Wire.isolated(partition, nodeId)
                && !Wire.isolated(partition, leaderId)
                && raft.getRole() == RaftState.Role.SEGUIDOR
                && raft.getKnownLeader() == leaderId
                && raft.getTerm() == electionTerm;
    }

    /** Registo exigido para os heartbeats publicados (não regista tentativas falhadas). */
    private void writeHeartbeat(Batch batch, String payload) throws IOException {
        String record = batch.getTerm() + ";" + batch.getHb() + ";"
                + System.currentTimeMillis() + ";" + batch.getLastSeq() + ";"
                + batch.getLastTerm() + ";" + batch.getRecords().size() + ";"
                + payload.getBytes(StandardCharsets.UTF_8).length + System.lineSeparator();
        synchronized (eventsLock) {
            Files.writeString(heartbeatsFile, record, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    /** Eventos no formato exigido: instante;evento;p<k>;detalhe. */
    private void logRaftTransitions(long prevTerm, RaftState.Role prevRole,
                                    int prevLeader, String source) {
        long nowTerm = raft.getTerm();
        RaftState.Role nowRole = raft.getRole();
        int nowLeader = raft.getKnownLeader();
        if (nowTerm > prevTerm) {
            if (prevRole == RaftState.Role.LIDER) {
                logEvent("DEMISSAO;termo=" + prevTerm + ";motivo=" + source);
            }
            logEvent("TERMO;de=" + prevTerm + ";para=" + nowTerm + ";origem=" + source);
        }
        if (nowLeader != 0 && nowLeader != prevLeader && nowLeader != nodeId) {
            logEvent("NOVO_LIDER;termo=" + nowTerm + ";lider=n" + nowLeader);
        }
    }

    private void logEvent(String event) {
        int split = event.indexOf(';');
        String kind = split < 0 ? event : event.substring(0, split);
        String details = split < 0 ? "" : event.substring(split + 1);
        // Eventos complementares antigos são classificados como ERRO/IGNORADO/RECEBIDO.
        if (kind.startsWith("ERRO_") || kind.endsWith("_ERRO_IO")
                || "SUBSCRICAO_FALHOU".equals(kind)) {
            details = kind + (details.isEmpty() ? "" : ";" + details);
            kind = "ERRO";
        } else if (kind.endsWith("_REJEITADO") || "MENSAGEM_REJEITADA".equals(kind)) {
            details = kind + (details.isEmpty() ? "" : ";" + details);
            kind = "IGNORADO";
        } else if ("IN_APLICADO".equals(kind)) {
            details = "IN_APLICADO;" + details;
            kind = "RECEBIDO";
        }
        String line = System.currentTimeMillis() + ";" + kind + ";p" + partition
                + ";" + details;
        synchronized (eventsLock) {
            try {
                Files.writeString(eventsFile, line + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Não foi possível guardar evento: " + e.getMessage());
            }
        }
        System.out.println("p" + partition + "-n" + nodeId + " " + line);
    }
}
