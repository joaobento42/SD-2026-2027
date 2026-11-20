package raft04;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

/** Confirma o evento de arranque sem depender de IPFS/Kubo. Executar numa pasta vazia. */
public final class PartitionNodeArranqueTest {
    private PartitionNodeArranqueTest() { }

    private static final class SilentTransport extends IpfsPubSub {
        SilentTransport() { super("http://127.0.0.1:5001/api/v0"); }
        @Override public void publish(String topic, String message) { }
        @Override public void subscribe(String topic, Consumer<String> onMessage)
                throws InterruptedException {
            // Mantém subscrições abertas até à interrupção no encerramento.
            while (!Thread.currentThread().isInterrupted()) Thread.sleep(50);
        }
    }

    public static void main(String[] args) throws Exception {
        if (Files.exists(Path.of("eventos-p0-n1.txt"))) {
            throw new IllegalStateException("Executar numa pasta temporária vazia");
        }
        try (PartitionNode node = new PartitionNode(0, 1, 3, "127.0.0.1",
                new SilentTransport())) {
            node.start();
            List<String> lines = Files.readAllLines(Path.of("eventos-p0-n1.txt"));
            long starts = lines.stream().filter(line -> line.matches("\\d+;ARRANQUE;p0;n=1")).count();
            if (starts != 1) {
                throw new AssertionError("Esperava um ARRANQUE no formato correto, encontrei " + starts);
            }
        }
        System.out.println("PartitionNodeArranqueTest: 1 verificação passou.");
    }
}
