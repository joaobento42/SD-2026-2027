package rep02;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;

    private Wire() { }

    public static String topicIn(int k) {
        return "rep02-in-p" + k;
    }

    public static String topicData(int k) {
        return "rep02-p" + k;
    }

    public static String in(String sensor, double temp) {
        return "IN;" + sensor + ";" + temp;
    }

    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
    }

    public static String type(String msg) {
        if (msg == null || msg.trim().isEmpty()) {
            throw new IllegalArgumentException("Mensagem vazia: " + msg);
        }

        String[] partes = msg.trim().split("[; ]", 2);
        String t = partes[0];

        if (t.isEmpty()) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }

        if (partes.length == 1 && !t.equals("FIM")) {
            throw new IllegalArgumentException("Mensagem sem delimitador ou tipo invalido: " + msg);
        }

        return t;
    }

    public static String body(String msg) {
        if (msg == null || msg.trim().isEmpty()) {
            return "";
        }

        String[] partes = msg.trim().split("[; ]", 2);
        return partes.length > 1 ? partes[1].trim() : "";
    }

    public static String[] regParts(String msg) {
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG")) {
            throw new IllegalArgumentException("REG mal formado: " + msg);
        }
        return new String[] { f[1], f[2], f[3] };
    }
}