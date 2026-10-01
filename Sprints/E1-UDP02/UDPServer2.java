import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer2 {

    private static final int PORT = 6789;
    private static final int BUFFER_SIZE = 1000;

    private static final List<String> receptionList = new ArrayList<>();
    private static final Map<Integer, String> temporaryMessages = new HashMap<>();

    public static void main(String[] args) {
        int lastInOrder = 0;

        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            System.out.println("Servidor UDP à escuta no porto " + PORT);

            while (true) {
                byte[] buffer = new byte[BUFFER_SIZE];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                String received = new String(
                        request.getData(),
                        request.getOffset(),
                        request.getLength(),
                        StandardCharsets.UTF_8
                );
                System.out.println();
                System.out.println("Recebi: " + received);

                String[] parts = received.split(",", 2);
                if (parts.length != 2) {
                    System.out.println("Datagrama ignorado: formato inválido.");
                    continue;
                }

                int sequence;
                try {
                    sequence = Integer.parseInt(parts[0].trim());
                } catch (NumberFormatException exception) {
                    System.out.println("Datagrama ignorado: número de sequência inválido.");
                    continue;
                }

                int previousLastInOrder = lastInOrder;
                lastInOrder = processDeliveredMessages(lastInOrder, sequence, parts[1]);

                String response;
                if (lastInOrder == previousLastInOrder) {
                    response = "waitingfor," + (lastInOrder + 1);
                } else {
                    response = received;
                }

                printServerState(previousLastInOrder, lastInOrder);
                System.out.println("Resposta: " + response);

                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                DatagramPacket reply = new DatagramPacket(
                        responseBytes,
                        responseBytes.length,
                        request.getAddress(),
                        request.getPort()
                );
                socket.send(reply);
            }
        } catch (SocketException exception) {
            System.out.println("Socket: " + exception.getMessage());
        } catch (IOException exception) {
            System.out.println("IO: " + exception.getMessage());
        }
    }

    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        if (nCurrentMessage == nLastMessageInOrder + 1) {
            receptionList.add(currentMessage);
            int lastDelivered = nCurrentMessage;
            System.out.println("Mensagem " + lastDelivered + " em ordem: entregue.");

            while (temporaryMessages.containsKey(lastDelivered + 1)) {
                lastDelivered++;
                receptionList.add(temporaryMessages.remove(lastDelivered));
                System.out.println("Mensagem " + lastDelivered + " retirada da estrutura temporária: entregue.");
            }
            return lastDelivered;
        }

        if (nCurrentMessage < 1) {
            System.out.println("Mensagem " + nCurrentMessage + " descartada: número de sequência inválido.");
        } else if (nCurrentMessage <= nLastMessageInOrder) {
            // Já foi entregue: se fosse guardada, ficaria para sempre na estrutura temporária.
            System.out.println("Mensagem " + nCurrentMessage + " repetida (já entregue): descartada.");
        } else if (temporaryMessages.containsKey(nCurrentMessage)) {
            System.out.println("Mensagem " + nCurrentMessage + " repetida (já está na estrutura temporária): ignorada.");
        } else {
            temporaryMessages.put(nCurrentMessage, currentMessage);
            System.out.println("Mensagem " + nCurrentMessage + " fora de ordem: guardada na estrutura temporária.");
        }
        return nLastMessageInOrder;
    }

    private static void printServerState(int previousLastInOrder, int lastInOrder) {
        System.out.println("L = " + lastInOrder);
        System.out.println("Estrutura temporária: " + temporaryMessages);
        System.out.println("Entregues neste passo: " + formatMessages(previousLastInOrder + 1, lastInOrder));
        System.out.println("Lista de receção: " + formatMessages(1, lastInOrder));
    }

    private static String formatMessages(int first, int last) {
        List<String> items = new ArrayList<>();
        for (int n = first; n <= last; n++) {
            items.add(n + "=" + receptionList.get(n - 1));
        }
        return items.toString();
    }
}
