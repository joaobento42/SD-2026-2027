package rep03;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    public static final int PORTO_ENTRADA_BASE = 7100;
    public static final double LIMIAR_ALARME = 28.0;
    public static final String PREFIXO_ALARME = "ALARM-";
    public static final String ENDERECO_LOCAL = "127.0.0.1";

    private Wire() { }

    public static String topicData(int k) {
        return "rep03-p" + k;
    }

    public static int portoRecuperacao(int k) {
        return PORTO_BASE + k;
    }

    public static int portoEntrada(int k) {
        return PORTO_ENTRADA_BASE + k;
    }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
    }

    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }

    public static String[] regParts(String msg) {
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG")) {
            throw new IllegalArgumentException("REG mal formado: " + msg);
        }
        return new String[] { f[1], f[2], f[3] };
    }

    public static final class In {
        public final String sensor;
        public final double temperature;
        public final long lamport;
        public final VectorClock vector;

        private In(String sensor, double temperature, long lamport, VectorClock vector) {
            this.sensor = sensor;
            this.temperature = temperature;
            this.lamport = lamport;
            this.vector = vector;
        }
    }

    public static In parseIn(String msg, int n) {
        if (!"IN".equals(type(msg))) {
            throw new IllegalArgumentException("Não é uma mensagem IN: " + msg);
        }
        String[] f = body(msg).split(";", -1);
        if (f.length != 4 || f[0].isBlank()) {
            throw new IllegalArgumentException("IN mal formado: " + msg);
        }
        double temp = Double.parseDouble(f[1]);
        long lamport = Long.parseLong(f[2]);
        if (Double.isNaN(temp) || Double.isInfinite(temp) || lamport < 0) {
            throw new IllegalArgumentException("IN com valores inválidos: " + msg);
        }
        return new In(f[0].trim(), temp, lamport, VectorClock.parse(f[3], n));
    }
}
