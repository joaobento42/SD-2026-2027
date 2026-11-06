package rep02;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionCheck {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }

        int n = Integer.parseInt(args[0]);
        File pasta = new File(args[1]);

        if (!pasta.isDirectory()) {
            System.out.println("Erro: A pasta especificada nao existe ou nao e um diretorio.");
            return;
        }

        Partitioner partitioner = new Partitioner(n);
        int somaRegistosParticoes = 0;
        boolean sistemaCompleto = true;

        for (int k = 0; k < n; k++) {
            final int particaoId = k;

            System.out.println("=== Verificacao da Particao " + particaoId + " ===");
            File ficheiroLider = new File(pasta, "lider-p" + particaoId + ".txt");

            if (!ficheiroLider.exists()) {
                System.out.println("FALHA: Ficheiro do lider (" + ficheiroLider.getName() + ") nao encontrado.");
                System.out.println("A particao " + particaoId + " nao pode ser verificada.\n");
                sistemaCompleto = false;
                continue;
            }

            List<SensorRecord> registosLider = lerFicheiro(ficheiroLider);
            int totalRegistos = registosLider.size();
            somaRegistosParticoes += totalRegistos;

            boolean chavesValidas = true;
            boolean sequenciaLiderValida = true;
            Set<String> sensoresDistintos = new HashSet<>();

            long seqEsperada = 1;
            for (SensorRecord r : registosLider) {
                if (partitioner.of(r.getSensorId()) != particaoId) {
                    chavesValidas = false;
                }
                if (r.getSeq() != seqEsperada) {
                    sequenciaLiderValida = false;
                }
                sensoresDistintos.add(r.getSensorId());
                seqEsperada++;
            }

            System.out.println("Ficheiro do lider: " + ficheiroLider.getName());
            System.out.println(" - Estado: " + (totalRegistos == 0 ? "Vazio" : "Contem dados"));
            System.out.println(" - Total de registos: " + totalRegistos);
            System.out.println(" - Total de sensores distintos: " + sensoresDistintos.size());
            System.out.println(" - Chaves pertencem todas a particao " + particaoId + ": " + (chavesValidas ? "Sim" : "Nao"));
            System.out.println(" - Sequencia (1 a N) sem saltos/repeticoes: " + (sequenciaLiderValida ? "Sim" : "Nao"));

            File[] ficheirosReplicas = pasta.listFiles((dir, name) -> name.matches("replica-\\d+-p" + particaoId + "\\.txt"));

            if (ficheirosReplicas == null) {
                System.out.println(" - Replicas que guardam esta particao: FALHA AO LER DIRETORIO");
            } else {
                System.out.println(" - Replicas que guardam esta particao: " + ficheirosReplicas.length + " (esperado: 2)");
                for (File ficheiroReplica : ficheirosReplicas) {
                    List<SensorRecord> registosReplica = lerFicheiro(ficheiroReplica);

                    boolean sequenciaReplicaValida = true;
                    long seqRepEsperada = 1;
                    for (SensorRecord r : registosReplica) {
                        if (r.getSeq() != seqRepEsperada) {
                            sequenciaReplicaValida = false;
                        }
                        seqRepEsperada++;
                    }

                    boolean prefixoOuIgual = true;
                    if (registosReplica.size() > totalRegistos) {
                        prefixoOuIgual = false;
                    } else {
                        for (int i = 0; i < registosReplica.size(); i++) {
                            if (!registosReplica.get(i).toLine().equals(registosLider.get(i).toLine())) {
                                prefixoOuIgual = false;
                                break;
                            }
                        }
                    }

                    int registosEmFalta = totalRegistos - registosReplica.size();

                    System.out.println("   * " + ficheiroReplica.getName() + ":");
                    System.out.println("     - Sequencia sem saltos/repeticoes: " + (sequenciaReplicaValida ? "Sim" : "Nao"));
                    System.out.println("     - E copia exata ou prefixo do lider: " + (prefixoOuIgual ? "Sim" : "Nao"));
                    System.out.println("     - Registos em falta: " + (registosEmFalta >= 0 ? registosEmFalta : "Ficheiro maior que o do lider!"));
                }
            }
            System.out.println();
        }

        System.out.println("=== Relatorio Global ===");
        if (sistemaCompleto) {
            System.out.println("Total de registos no sistema (soma de todas as particoes): " + somaRegistosParticoes);
            System.out.println("Confirmacao: O total do sistema equivale rigorosamente a soma dos registos validos de cada particao.");
        } else {
            System.out.println("Total parcial de registos no sistema (apenas particoes encontradas): " + somaRegistosParticoes);
            System.out.println("Aviso: O total global esta incompleto e inconfirmavel devido a falta de ficheiros de lider.");
        }
    }

    private static List<SensorRecord> lerFicheiro(File ficheiro) throws Exception {
        List<SensorRecord> lista = new ArrayList<>();
        List<String> linhas = Files.readAllLines(ficheiro.toPath());
        for (String linha : linhas) {
            if (!linha.isBlank()) {
                lista.add(SensorRecord.fromLine(linha));
            }
        }
        return lista;
    }
}