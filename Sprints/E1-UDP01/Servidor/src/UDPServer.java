import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class UDPServer {

    private static final int PORT = 6789;
    private static final int BUFFER_SIZE = 1000;

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

                String response;
                if (sequence != lastInOrder + 1) {
                    response = "waitingfor," + (lastInOrder + 1);
                } else {
                    lastInOrder = sequence;
                    response = received;
                }

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
}
