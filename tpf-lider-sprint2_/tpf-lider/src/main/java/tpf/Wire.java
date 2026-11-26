package tpf;


public final class Wire {


    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMaxBytes", 65536);


    public static final int PAD_BYTES = Integer.getInteger("tpf.padBytes", 4096);


    public static final long SELAGEM_MS = Long.getLong("tpf.selagemMs", 5000L);

    private Wire() {
    }
}
