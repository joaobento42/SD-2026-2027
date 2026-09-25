import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class UDPClient {

    private static final int SERVER_PORT = 6789;
    private static final int BUFFER_SIZE = 1000;
    private static final String EXIT_WORD = "sair";

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket();
             Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {

            InetAddress serverAddress = InetAddress.getByName("localhost");
            boolean automaticMode = chooseMode(scanner);
            int nextAutomaticSequence = 1;

            System.out.println("Escreva mensagens. Para terminar, escreva '" + EXIT_WORD + "'.");

            while (true) {
                int sequence;
                String message;

                if (automaticMode) {
                    message = readMessage(scanner);
                    if (message == null) {
                        break;
                    }
                    sequence = nextAutomaticSequence++;
                } else {
                    String input = readMessage(scanner);
                    if (input == null) {
                        break;
                    }

                    int separator = input.indexOf(' ');
                    if (separator <= 0 || separator == input.length() - 1) {
                        System.out.println("No modo manual, use: <número> <mensagem>");
                        continue;
                    }

                    try {
                        sequence = Integer.parseInt(input.substring(0, separator));
                    } catch (NumberFormatException exception) {
                        System.out.println("Número de sequência inválido.");
                        continue;
                    }
                    message = input.substring(separator + 1);
                }

                String requestText = sequence + "," + message;
                byte[] requestBytes = requestText.getBytes(StandardCharsets.UTF_8);
                DatagramPacket request = new DatagramPacket(
                        requestBytes,
                        requestBytes.length,
                        serverAddress,
                        SERVER_PORT
                );
                socket.send(request);

                byte[] responseBuffer = new byte[BUFFER_SIZE];
                DatagramPacket reply = new DatagramPacket(responseBuffer, responseBuffer.length);
                socket.receive(reply);

                String response = new String(
                        reply.getData(),
                        reply.getOffset(),
                        reply.getLength(),
                        StandardCharsets.UTF_8
                );
                if (response.startsWith("waitingfor,")) {
                    System.out.println("WAITINGFOR: " + response.substring("waitingfor,".length()));
                } else {
                    System.out.println("ECHO: " + response);
                }
            }
        } catch (SocketException exception) {
            System.out.println("Socket: " + exception.getMessage());
        } catch (IOException exception) {
            System.out.println("IO: " + exception.getMessage());
        }
    }

    private static boolean chooseMode(Scanner scanner) {
        while (true) {
            System.out.print("Escolha o modo (a=automático, m=manual): ");
            String mode = scanner.nextLine().trim().toLowerCase();
            if ("a".equals(mode)) {
                return true;
            }
            if ("m".equals(mode)) {
                return false;
            }
            System.out.println("Modo inválido.");
        }
    }

    private static String readMessage(Scanner scanner) {
        System.out.print("> ");
        if (!scanner.hasNextLine()) {
            return null;
        }

        String message = scanner.nextLine();
        if (EXIT_WORD.equalsIgnoreCase(message.trim())) {
            return null;
        }
        return message;
    }
}
