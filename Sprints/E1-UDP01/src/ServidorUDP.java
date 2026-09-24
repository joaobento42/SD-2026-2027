import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ServidorUDP {
    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket(6789)) {
            byte[] buffer = new byte[1024];

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                String mensagemRecebida = new String(request.getData(), 0, request.getLength(), StandardCharsets.UTF_8);

                String resposta = "Recebido: " + mensagemRecebida;
                byte[] responseData = resposta.getBytes(StandardCharsets.UTF_8);

                DatagramPacket reply = new DatagramPacket(
                        responseData,
                        responseData.length,
                        request.getAddress(),
                        request.getPort()
                );

                socket.send(reply);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}