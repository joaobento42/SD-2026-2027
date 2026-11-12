package raft04;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Testes de protocolo sem dependência de framework. */
public final class ElectionProtocolTest {
    private static int checks;

    private static void check(boolean ok, String message) {
        checks++;
        if (!ok) {
            throw new AssertionError(message);
        }
    }

    private static void invalid(Runnable action, String message) {
        checks++;
        try {
            action.run();
        } catch (IllegalArgumentException e) {
            return;
        }
        throw new AssertionError("Deveria rejeitar: " + message);
    }

    public static void main(String[] args) throws IOException {
        VoteRequest request = new VoteRequest(2, 3, 15, 1);
        check(request.encode().equals("PEDIDO_VOTO;2;3;15;1"), "encode PEDIDO_VOTO");
        VoteRequest parsedRequest = VoteRequest.decode(request.encode());
        check(parsedRequest.term == 2, "termo do pedido");
        check(parsedRequest.candidate == 3, "candidato do pedido");
        check(parsedRequest.lastSeq == 15, "lastSeq do pedido");
        check(parsedRequest.lastTerm == 1, "lastTerm do pedido");
        check(VoteRequest.decode("PEDIDO_VOTO;0;1;0;0").lastSeq == 0, "zeros válidos");
        invalid(() -> VoteRequest.decode(null), "pedido nulo");
        invalid(() -> VoteRequest.decode("VOTO;2;3;15;1"), "tipo incorreto");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;2;3"), "campos insuficientes");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;2;3;4;"), "campos a mais");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;x;2;3;4"), "termo não numérico");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;-1;2;3;4"), "termo negativo");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;0;3;4"), "candidato zero");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;4;3;4"), "candidato fora do grupo");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;2;-1;4"), "seq negativo");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;2;3;-4"), "último termo negativo");
        invalid(() -> VoteRequest.decode("PEDIDO_VOTO;1;2;;4"), "campo vazio");

        VoteReply yes = new VoteReply(2, 1, 3, true);
        VoteReply no = new VoteReply(2, 1, 3, false);
        check(yes.encode().equals("VOTO;2;1;3;S"), "encode voto concedido");
        check(no.encode().equals("VOTO;2;1;3;N"), "encode voto recusado");
        VoteReply parsedYes = VoteReply.decode(yes.encode());
        check(parsedYes.term == 2, "termo da resposta");
        check(parsedYes.voter == 1, "eleitor da resposta");
        check(parsedYes.candidate == 3, "candidato da resposta");
        check(parsedYes.granted, "decisão S");
        check(!VoteReply.decode(no.encode()).granted, "decisão N");
        invalid(() -> VoteReply.decode(null), "resposta nula");
        invalid(() -> VoteReply.decode("PEDIDO_VOTO;2;1;3;S"), "tipo incorreto");
        invalid(() -> VoteReply.decode("VOTO;2;1;3"), "campos insuficientes");
        invalid(() -> VoteReply.decode("VOTO;2;1;3;S;"), "campos a mais");
        invalid(() -> VoteReply.decode("VOTO;x;1;3;S"), "termo não numérico");
        invalid(() -> VoteReply.decode("VOTO;-1;1;3;S"), "termo negativo");
        invalid(() -> VoteReply.decode("VOTO;2;0;3;S"), "eleitor zero");
        invalid(() -> VoteReply.decode("VOTO;2;4;3;S"), "eleitor fora do grupo");
        invalid(() -> VoteReply.decode("VOTO;2;1;0;S"), "candidato zero");
        invalid(() -> VoteReply.decode("VOTO;2;1;4;S"), "candidato fora do grupo");
        invalid(() -> VoteReply.decode("VOTO;2;1;3;X"), "decisão desconhecida");
        invalid(() -> VoteReply.decode("VOTO;2;1;3;s"), "decisão minúscula");
        invalid(() -> VoteReply.decode("VOTO;2;1;;S"), "campo vazio");

        check(Wire.topicElection(0).equals("raft04-eleicao-p0"), "tópico p0");
        check(Wire.topicElection(1).equals("raft04-eleicao-p1"), "tópico p1");
        check(Wire.topicElection(2).equals("raft04-eleicao-p2"), "tópico p2");
        invalid(() -> Wire.topicElection(-1), "tópico -1");
        invalid(() -> Wire.topicElection(3), "tópico 3");
        invalid(() -> Wire.isolated(-1, 1), "isolamento partição -1");
        invalid(() -> Wire.isolated(3, 1), "isolamento partição 3");
        invalid(() -> Wire.isolated(0, 0), "isolamento nó 0");
        invalid(() -> Wire.isolated(0, 4), "isolamento nó fora do grupo");

        Path flag = Path.of("isolar-p2-n3");
        if (Files.exists(flag)) {
            throw new IllegalStateException("Teste não pode alterar ficheiro já existente: " + flag);
        }
        check(!Wire.isolated(2, 3), "não isolado sem ficheiro");
        try {
            Files.createFile(flag);
            check(Wire.isolated(2, 3), "isolado com ficheiro");
        } finally {
            Files.deleteIfExists(flag);
        }
        check(!Wire.isolated(2, 3), "não isolado após apagar ficheiro");
        System.out.println("OK: " + checks + " verificações do protocolo eleitoral");
    }
}
