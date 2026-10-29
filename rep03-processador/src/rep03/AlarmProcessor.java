package rep03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public final class AlarmProcessor {
    private static final String FALHA = System.getProperty("falha", "");
    private static final long ESPERA_FALHA_MS = 50;

    private final int n;
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final LeaderClient lideres;

    private long lp;
    private final VectorClock vp;

    private final AtomicInteger alarmesEnviados = new AtomicInteger();

    private final List<Set<Long>> vistos = new ArrayList<>();

    private AlarmProcessor(int n, String api, String enderecosLideres) {
        this.n = n;
        this.partitioner = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.lideres = new LeaderClient(n, enderecosLideres);
        this.vp = new VectorClock(n);
        for (int k = 0; k < n; k++) {
            vistos.add(new HashSet<>());
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api] [endereçosLíderes]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        String enderecos = args.length > 2 ? args[2] : null;

        new AlarmProcessor(n, api, enderecos).iniciar();
    }

    private void iniciar() {
        System.out.println("Processador de alarmes: N = " + n + ", limiar " + Wire.LIMIAR_ALARME + " °C");
        if (!FALHA.isEmpty()) {
            System.out.println("ATENÇÃO: falha simulada ativa: " + FALHA);
        }
        for (int k = 0; k < n; k++) {
            final int particao = k;
            new Thread(() -> ps.subscribeForever(Wire.topicData(particao), msg -> aoReceber(particao, msg)),
                    "processador-p" + k).start();
        }
    }

    private void aoReceber(int k, String msg) {
        SensorRecord r;
        try {
            r = SensorRecord.fromLine(Wire.regParts(msg)[2], n);
        } catch (RuntimeException e) {
            System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
            return;
        }

        if (r.getSensorId().startsWith(Wire.PREFIXO_ALARME) || !vistos.get(k).add(r.getSeq())) {
            return;
        }

        long lAlarme;
        VectorClock vAlarme;
        synchronized (this) {
            lp = Math.max(lp, r.getLamport());
            vp.merge(r.getVector());

            if (r.getTemperature() < Wire.LIMIAR_ALARME) {
                return;
            }
            lAlarme = lp;
            vAlarme = "sem-copy".equals(FALHA) ? vp : vp.copy();
        }

        if ("sem-copy".equals(FALHA)) {
            pausa();
        }

        String sensorAlarme = Wire.PREFIXO_ALARME + r.getSensorId();
        int j = partitioner.of(sensorAlarme);
        String in = Wire.in(sensorAlarme, r.getTemperature(), lAlarme, vAlarme);
        try {
            lideres.send(j, in);
            System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + j
                    + "  (L=" + lAlarme + " V=" + in.substring(in.lastIndexOf(';') + 1)
                    + ", enviados " + alarmesEnviados.incrementAndGet() + ")");
        } catch (IOException e) {
            System.out.println("ALARME PERDIDO: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + j
                    + " (" + e.getMessage() + ")");
        }
    }

    private static void pausa() {
        try {
            Thread.sleep(ESPERA_FALHA_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
