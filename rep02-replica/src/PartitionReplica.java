package rep02;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class PartitionReplica {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <particoes> [portoApi]");
            return;
        }

        int id = Integer.parseInt(args[0]);
        String[] particoesStr = args[1].split(",");
        int[] particoes = new int[particoesStr.length];
        for (int i = 0; i < particoesStr.length; i++) {
            particoes[i] = Integer.parseInt(particoesStr[i].trim());
        }

        String portoApi = args.length > 2 ? args[2] : "5001";
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";

        double perda = Double.parseDouble(System.getProperty("perda", "0.0"));
        if (Double.isNaN(perda) || perda < 0.0 || perda > 1.0) {
            throw new IllegalArgumentException("O valor da propriedade perda deve estar entre 0.0 e 1.0");
        }

        long descartar = Long.parseLong(System.getProperty("descartar", "-1"));

        IpfsPubSub ps = new IpfsPubSub(api);

        for (int k : particoes) {
            iniciarSubscricaoParticao(id, k, ps, perda, descartar);
        }
    }

    private static void iniciarSubscricaoParticao(int id, int k, IpfsPubSub ps, double perda, long descartar) {
        new Thread(() -> {
            try {
                RecordFile file = new RecordFile("replica-" + id + "-p" + k + ".txt");
                long[] estadoSeq = new long[1];
                estadoSeq[0] = file.lastSeq();
                TreeMap<Long, SensorRecord> retidos = new TreeMap<>();

                String[] enderecoLider = new String[1];
                int[] portoLider = new int[1];
                boolean[] jaDescartou = new boolean[1];

                ps.subscribe(Wire.topicData(k), msg -> {
                    try {
                        String tipo = Wire.type(msg);
                        if (!"REG".equals(tipo)) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        String corpo = Wire.body(msg);
                        String[] partes = corpo.split(";", 3);
                        if (partes.length < 3) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        String endereco = partes[0];
                        int porto = Integer.parseInt(partes[1]);
                        String linhaRegisto = partes[2];

                        SensorRecord r;
                        try {
                            r = SensorRecord.fromLine(linhaRegisto);
                        } catch (Exception e) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        long seqRecebido = r.getSeq();

                        if (Math.random() < perda) {
                            System.out.println("DESCARTADO (simulação): " + seqRecebido);
                            return;
                        }

                        if (seqRecebido == descartar && !jaDescartou[0]) {
                            jaDescartou[0] = true;
                            System.out.println("DESCARTADO (simulação): " + seqRecebido);
                            return;
                        }

                        synchronized (file) {
                            enderecoLider[0] = endereco;
                            portoLider[0] = porto;

                            if (seqRecebido <= estadoSeq[0] || retidos.containsKey(seqRecebido)) {
                                System.out.println("DUPLICADO: " + msg);
                            } else if (seqRecebido == estadoSeq[0] + 1) {
                                file.append(r);
                                estadoSeq[0] = seqRecebido;
                                processarRetidos(file, estadoSeq, retidos);
                            } else {
                                System.out.println("EM ESPERA: " + msg);
                                retidos.put(seqRecebido, r);
                            }

                            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                                long de = estadoSeq[0] + 1;
                                long ate = retidos.firstKey() - 1;
                                if (de <= ate) {
                                    System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + enderecoLider[0] + ":" + portoLider[0]);
                                    recuperarTCP(enderecoLider[0], portoLider[0], de, ate, file, estadoSeq, retidos);
                                    processarRetidos(file, estadoSeq, retidos);
                                }
                            }
                        }

                    } catch (Exception e) {
                        System.out.println("Erro ao processar mensagem PUB na partição " + k + ": " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.out.println("Erro na thread da partição " + k + ": " + e.getMessage());
            }
        }).start();
    }

    private static void processarRetidos(RecordFile file, long[] estadoSeq, TreeMap<Long, SensorRecord> retidos) throws Exception {
        while (!retidos.isEmpty() && retidos.firstKey() == estadoSeq[0] + 1) {
            Map.Entry<Long, SensorRecord> entry = retidos.firstEntry();
            file.append(entry.getValue());
            estadoSeq[0] = entry.getKey();
            retidos.remove(entry.getKey());
        }
    }

    private static void recuperarTCP(String endereco, int porto, long de, long ate, RecordFile file, long[] estadoSeq, TreeMap<Long, SensorRecord> retidos) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(endereco, porto), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);

            try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                out.println("PEDIDO " + de + " " + ate);

                String linha;
                boolean fimRecebido = false;
                boolean erroDetetado = false;

                while ((linha = in.readLine()) != null) {
                    if ("FIM".equals(linha)) {
                        fimRecebido = true;
                        break;
                    }
                    if (linha.startsWith("ERRO")) {
                        System.out.println("RECUPERAÇÃO FALHOU: " + linha);
                        erroDetetado = true;
                        break;
                    }

                    try {
                        SensorRecord r = SensorRecord.fromLine(linha);
                        long seqRec = r.getSeq();

                        if (seqRec == estadoSeq[0] + 1) {
                            file.append(r);
                            estadoSeq[0] = seqRec;
                            retidos.remove(seqRec);
                            System.out.println("RECUPERADO: " + seqRec);
                        } else if (seqRec > estadoSeq[0] + 1 && !retidos.containsKey(seqRec)) {
                            retidos.put(seqRec, r);
                        }
                    } catch (Exception e) {
                        System.out.println("RECUPERAÇÃO FALHOU: Erro a processar registo recuperado - " + e.getMessage());
                        erroDetetado = true;
                        break;
                    }
                }

                if (!fimRecebido && !erroDetetado) {
                    System.out.println("RECUPERAÇÃO FALHOU: A ligacao foi encerrada prematuramente sem a mensagem FIM.");
                } else if (fimRecebido && estadoSeq[0] < ate) {
                    System.out.println("RECUPERAÇÃO FALHOU: Resposta incompleta, faltam registos ate " + ate);
                }

            }
        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
    }
}