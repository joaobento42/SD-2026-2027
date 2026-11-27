package tpf;

import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkCryptoTest {

    private static final List<Registo> REGISTOS = List.of(
            new Registo(1, 1_700_000_000_000L, "ana", "arquivo", "olá mundo"),
            new Registo(2, 1_700_000_000_500L, "rui", "arquivo", "segundo registo"));

    private static byte[] chave(int semente) {
        byte[] k = new byte[32];
        new Random(semente).nextBytes(k);
        return k;
    }

    private static byte[] chunkComPadding() {
        Chunk c = new Chunk();
        REGISTOS.forEach(c::adicionar);
        return c.serializar(4096);
    }

    private static byte[] alterarUmByte(byte[] original) {
        byte[] copia = original.clone();
        copia[0] ^= 0x01;
        return copia;
    }

    @Test
    void cifrarEDecifrar() throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(chave(1));
        byte[] texto = chunkComPadding();
        ChunkCrypto.Sealed s = crypto.seal(texto);

        byte[] decifrado = crypto.open(s);
        assertArrayEquals(texto, decifrado);
        assertEquals(REGISTOS, Chunk.ler(decifrado));
        assertEquals(ChunkCrypto.sha256Hex(s.ciphertext()), s.id());
        assertEquals(texto.length + 16, s.ciphertext().length);
        assertEquals(32, s.salt().length);
        assertEquals(12, s.nonce().length);
        assertEquals(1, s.keyVersion());
    }

    @Test
    void byteAlteradoNoTextoCifradoFalha() throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(chave(1));
        ChunkCrypto.Sealed s = crypto.seal(chunkComPadding());
        ChunkCrypto.Sealed alterado = new ChunkCrypto.Sealed(s.id(), s.keyVersion(), s.salt(), s.nonce(),
                alterarUmByte(s.ciphertext()));
        GeneralSecurityException e = assertThrows(GeneralSecurityException.class, () -> crypto.open(alterado));
        assertTrue(e.getMessage().startsWith("O SHA-256"));
    }

    @Test
    void saltDiferenteFalha() throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(chave(1));
        ChunkCrypto.Sealed s = crypto.seal(chunkComPadding());
        ChunkCrypto.Sealed outro = new ChunkCrypto.Sealed(s.id(), s.keyVersion(), alterarUmByte(s.salt()),
                s.nonce(), s.ciphertext());
        assertThrows(AEADBadTagException.class, () -> crypto.open(outro));
    }

    @Test
    void nonceDiferenteFalha() throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(chave(1));
        ChunkCrypto.Sealed s = crypto.seal(chunkComPadding());
        ChunkCrypto.Sealed outro = new ChunkCrypto.Sealed(s.id(), s.keyVersion(), s.salt(),
                alterarUmByte(s.nonce()), s.ciphertext());
        assertThrows(AEADBadTagException.class, () -> crypto.open(outro));
    }

    @Test
    void keyVersionDiferenteFalha() throws Exception {
        ChunkCrypto crypto = new ChunkCrypto(chave(1));
        ChunkCrypto.Sealed s = crypto.seal(chunkComPadding());
        ChunkCrypto.Sealed outro = new ChunkCrypto.Sealed(s.id(), 2, s.salt(), s.nonce(), s.ciphertext());
        GeneralSecurityException e = assertThrows(GeneralSecurityException.class, () -> crypto.open(outro));
        assertTrue(e.getMessage().startsWith("Versão de chave"));
    }

    @Test
    void masterKeyDiferenteFalha() throws Exception {
        ChunkCrypto.Sealed s = new ChunkCrypto(chave(1)).seal(chunkComPadding());
        ChunkCrypto outraChave = new ChunkCrypto(chave(2));
        assertThrows(AEADBadTagException.class, () -> outraChave.open(s));
    }
}
