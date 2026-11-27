package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

public final class ChunkCrypto {

    public static final int KEY_VERSION = 1;
    public static final int TAG_BITS = 128;

    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] AAD_PREFIXO = "tpf-v1".getBytes(StandardCharsets.US_ASCII);

    private final byte[] masterKey;
    private final SecureRandom aleatorio = new SecureRandom();

    public ChunkCrypto(byte[] masterKey) {
        this.masterKey = masterKey;
    }


    public Sealed seal(byte[] textoComPadding) throws GeneralSecurityException {
        byte[] salt = new byte[32];
        byte[] nonce = new byte[12];
        aleatorio.nextBytes(salt);
        aleatorio.nextBytes(nonce);

        Cipher cifra = Cipher.getInstance("AES/GCM/NoPadding");
        cifra.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(hkdf(masterKey, salt, INFO), "AES"),
                new GCMParameterSpec(TAG_BITS, nonce));
        cifra.updateAAD(aad(KEY_VERSION, salt, nonce));
        byte[] cifrado = cifra.doFinal(textoComPadding);
        return new Sealed(sha256Hex(cifrado), KEY_VERSION, salt, nonce, cifrado);
    }





    public byte[] open(Sealed s) throws GeneralSecurityException {
        if (!sha256Hex(s.ciphertext()).equals(s.id())) {
            throw new GeneralSecurityException("O SHA-256 do conteúdo não corresponde ao chunkId " + s.id());
        }
        if (s.keyVersion() != KEY_VERSION) {
            throw new GeneralSecurityException("Versão de chave desconhecida: " + s.keyVersion());
        }
        Cipher cifra = Cipher.getInstance("AES/GCM/NoPadding");
        cifra.init(Cipher.DECRYPT_MODE, new SecretKeySpec(hkdf(masterKey, s.salt(), INFO), "AES"),
                new GCMParameterSpec(TAG_BITS, s.nonce()));
        cifra.updateAAD(aad(s.keyVersion(), s.salt(), s.nonce()));
        return cifra.doFinal(s.ciphertext());
    }


    static byte[] hkdf(byte[] chave, byte[] salt, byte[] info) throws GeneralSecurityException {
        Mac hmac = Mac.getInstance("HmacSHA256");
        hmac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = hmac.doFinal(chave);

        hmac.init(new SecretKeySpec(prk, "HmacSHA256"));
        hmac.update(info);
        hmac.update((byte) 0x01);
        return hmac.doFinal();
    }


    static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        return ByteBuffer.allocate(AAD_PREFIXO.length + 4 + salt.length + nonce.length)
                .put(AAD_PREFIXO).putInt(keyVersion).put(salt).put(nonce).array();
    }

    public static String sha256Hex(byte[] dados) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
    }





    public static byte[] carregarOuGerarMasterKey(Path ficheiro) throws IOException, GeneralSecurityException {
        if (!Files.exists(ficheiro)) {
            byte[] nova = new byte[32];
            new SecureRandom().nextBytes(nova);
            Files.write(ficheiro, nova);
            soLeituraParaODono(ficheiro);
        }
        byte[] mk = Files.readAllBytes(ficheiro);
        if (mk.length != 32) {
            throw new GeneralSecurityException(ficheiro + " tem " + mk.length + " bytes em vez de 32: arranque recusado");
        }
        return mk;
    }

    private static void soLeituraParaODono(Path ficheiro) throws IOException {
        if (ficheiro.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            Files.setPosixFilePermissions(ficheiro, PosixFilePermissions.fromString("r--------"));
        } else {

            AclFileAttributeView acl = Files.getFileAttributeView(ficheiro, AclFileAttributeView.class);
            acl.setAcl(List.of(AclEntry.newBuilder()
                    .setType(AclEntryType.ALLOW)
                    .setPrincipal(Files.getOwner(ficheiro))
                    .setPermissions(AclEntryPermission.READ_DATA, AclEntryPermission.READ_ATTRIBUTES,
                            AclEntryPermission.READ_NAMED_ATTRS, AclEntryPermission.READ_ACL,
                            AclEntryPermission.SYNCHRONIZE, AclEntryPermission.DELETE)
                    .build()));
        }
    }





    public record Sealed(String id, int keyVersion, byte[] salt, byte[] nonce, byte[] ciphertext) {

        public void gravar(Path pasta) throws IOException {
            Base64.Encoder b64 = Base64.getEncoder();
            Files.write(pasta.resolve(id + ".bin"), ciphertext);
            Files.writeString(pasta.resolve(id + ".hdr"),
                    keyVersion + ";" + b64.encodeToString(salt) + ";" + b64.encodeToString(nonce));
        }

        public static Sealed ler(Path pasta, String id) throws IOException {
            String[] hdr = Files.readString(pasta.resolve(id + ".hdr")).trim().split(";");
            Base64.Decoder b64 = Base64.getDecoder();
            return new Sealed(id, Integer.parseInt(hdr[0]), b64.decode(hdr[1]), b64.decode(hdr[2]),
                    Files.readAllBytes(pasta.resolve(id + ".bin")));
        }
    }
}
