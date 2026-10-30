package rep03;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class PartitionCheck {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }
        int n = Integer.parseInt(args[0]);
        File pasta = new File(args[1]);
        if (!pasta.isDirectory()) {
            System.out.println("Erro: a pasta especificada não existe ou não é um diretório.");
            return;
        }

        Partitioner partitioner = new Partitioner(n);
        int total = 0;
        boolean completo = true;

        for (int k = 0; k < n; k++) {
            System.out.println("=== Partição " + k + " ===");
            File ficheiroLider = new File(pasta, "lider-p" + k + ".txt");
            if (!ficheiroLider.exists()) {
                System.out.println("FALHA: " + ficheiroLider.getName() + " não encontrado.\n");
                completo = false;
                continue;
            }

            List<SensorRecord> lider = new RecordFile(ficheiroLider.getPath()).readAll();
            total += lider.size();

            boolean chavesValidas = true;
            Set<String> sensores = new HashSet<>();
            for (SensorRecord r : lider) {
                if (partitioner.of(r.getSensorId()) != k) {
                    chavesValidas = false;
                }
                sensores.add(r.getSensorId());
            }

            System.out.println("Ficheiro do líder: " + ficheiroLider.getName());
            System.out.println(" - registos: " + lider.size());
            System.out.println(" - sensores distintos: " + sensores.size());
            System.out.println(" - todos os sensores pertencem a p" + k + ": " + simNao(chavesValidas));
            System.out.println(" - sequência de 1 a " + lider.size() + " sem saltos nem repetições: " + simNao(sequenciaValida(lider)));

            final int particao = k;
            File[] replicas = pasta.listFiles((dir, nome) -> nome.matches("replica-\\d+-p" + particao + "\\.txt"));
            if (replicas == null) {
                System.out.println(" - réplicas: falha ao ler a pasta");
                continue;
            }
            System.out.println(" - réplicas que guardam a partição: " + replicas.length + " (esperado: 2)");
            for (File f : replicas) {
                List<SensorRecord> replica = new RecordFile(f.getPath()).readAll();
                boolean prefixo = replica.size() <= lider.size();
                for (int i = 0; prefixo && i < replica.size(); i++) {
                    prefixo = replica.get(i).toLine().equals(lider.get(i).toLine());
                }
                int emFalta = lider.size() - replica.size();
                System.out.println("   * " + f.getName() + ":");
                System.out.println("     - sequência sem saltos nem repetições: " + simNao(sequenciaValida(replica)));
                System.out.println("     - igual ao líder ou prefixo dele: " + simNao(prefixo)
                        + (prefixo && emFalta == 0 ? " (igual)" : ""));
                System.out.println("     - registos em falta: " + (emFalta >= 0 ? emFalta : "ficheiro maior que o do líder!"));
            }
            System.out.println();
        }

        System.out.println("=== Relatório global ===");
        if (completo) {
            System.out.println("Total de registos no sistema (soma das partições): " + total);
        } else {
            System.out.println("Total parcial (faltam ficheiros de líder): " + total);
        }
    }

    private static boolean sequenciaValida(List<SensorRecord> registos) {
        for (int i = 0; i < registos.size(); i++) {
            if (registos.get(i).getSeq() != i + 1) {
                return false;
            }
        }
        return true;
    }

    private static String simNao(boolean b) {
        return b ? "Sim" : "Não";
    }
}
