/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.influxdb.v3.client.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Base64;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class UtilsTest {


    private final Path tempDir;

    UtilsTest(@TempDir final Path tempDir) {
        this.tempDir = tempDir;
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0",
            "1",
            "123",
            "+123",
            "-1",
            "-123",
            "2147483647",
            "-2147483648"
    })
    void isIntegerValid(final String value) {
        Assertions.assertThat(Utils.isInteger(value)).isTrue();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            " ",
            "   ",
            "\t",
            "\n",
            "abc",
            "12a",
            "a12",
            "1 2",
            "1.0",
            "-1.5",
            "1e5",
            "2147483648",
            "-2147483649",
            "99999999999999999999"
    })
    void isIntegerInvalid(final String value) {
        Assertions.assertThat(Utils.isInteger(value)).isFalse();
    }

    @Test
    void testEmpty() {
        Assertions.assertThat(Utils.isInteger("")).isFalse();
    }

    @Test
    void loadPrivateKeyMissingBeginHeader() throws IOException {
        Path keyFile = tempDir.resolve("missing_begin.key");
        Files.writeString(keyFile,
                "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3\n-----END PRIVATE KEY-----");

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid private key format. Private key must be in PKCS#8 format.");
    }

    @Test
    void loadPrivateKeyMissingEndHeader() throws IOException {
        Path keyFile = tempDir.resolve("missing_end.key");
        Files.writeString(keyFile,
                "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3");

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid private key format. Private key must be in PKCS#8 format.");
    }

    @Test
    void loadPrivateKeyMissingBothHeaders() throws IOException {
        Path keyFile = tempDir.resolve("missing_both.key");
        Files.writeString(keyFile, "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3");

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid private key format. Private key must be in PKCS#8 format.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"RSA", "EC", "DSA"})
    void loadPrivateKeySupportedAlgorithms(final String algorithm) throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(algorithm);
        if ("RSA".equals(algorithm)) {
            keyPairGen.initialize(2048);
        } else if ("EC".equals(algorithm)) {
            keyPairGen.initialize(256);
        } else if ("DSA".equals(algorithm)) {
            keyPairGen.initialize(1024);
        }
        KeyPair keyPair = keyPairGen.generateKeyPair();
        String encodedKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        String pem = "-----BEGIN PRIVATE KEY-----\n" + encodedKey + "\n-----END PRIVATE KEY-----\n";
        Path keyFile = tempDir.resolve("valid_" + algorithm + ".key");
        Files.writeString(keyFile, pem);

        PrivateKey privateKey = Utils.loadPrivateKey(keyFile.toString());
        Assertions.assertThat(privateKey).isNotNull();
        Assertions.assertThat(privateKey.getAlgorithm()).isEqualTo(algorithm);
    }

    @Test
    void loadPrivateKeyUnsupportedAlgorithm() throws Exception {
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("Ed25519");
        KeyPair keyPair = keyPairGen.generateKeyPair();
        String encodedKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        String pem = "-----BEGIN PRIVATE KEY-----\n" + encodedKey + "\n-----END PRIVATE KEY-----\n";
        Path keyFile = tempDir.resolve("ed25519.key");
        Files.writeString(keyFile, pem);

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessage("Unsupported private key algorithm")
                .hasCauseInstanceOf(GeneralSecurityException.class);
    }

    @Test
    void loadPrivateKeyInvalidKeyBytesThrowsUnsupportedAlgorithm() throws Exception {
        String encodedKey = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        String pem = "-----BEGIN PRIVATE KEY-----\n" + encodedKey + "\n-----END PRIVATE KEY-----\n";
        Path keyFile = tempDir.resolve("invalid_bytes.key");
        Files.writeString(keyFile, pem);

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessage("Unsupported private key algorithm")
                .hasCauseInstanceOf(GeneralSecurityException.class);
    }

    @Test
    void loadPrivateKeyEmptyFile() throws IOException {
        Path keyFile = tempDir.resolve("empty.key");
        Files.writeString(keyFile, "");

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid private key format. Private key must be in PKCS#8 format.");
    }

    @Test
    void loadPrivateKeyEncryptedPkcs8Rejected() throws IOException {
        Path keyFile = tempDir.resolve("encrypted_pkcs8.key");
        Files.writeString(keyFile,
                "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIE...\n-----END ENCRYPTED PRIVATE KEY-----");

        Assertions.assertThatThrownBy(() -> Utils.loadPrivateKey(keyFile.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Encrypted PKCS#8 private keys are not supported. "
                        + "Use an unencrypted PKCS#8 key or a PKCS#12 file.");
    }

    @Test
    void createTmfUnsupportedType() throws IOException {
        Path keyFile = tempDir.resolve("cert.java");
        Assertions.assertThatThrownBy(() -> Utils.createTmf(keyFile.toString(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported certificate format");
    }

    @Test
    void getX509TrustManagerDefault() throws Exception {
        X509TrustManager trustManager = Utils.getX509TrustManager(null);
        Assertions.assertThat(trustManager).isNotNull();
    }

    @Test
    void getX509TrustManagerFromFactory() throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null);
        X509TrustManager trustManager = Utils.getX509TrustManager(tmf);
        Assertions.assertThat(trustManager).isNotNull();
    }

    @Test
    void buildSslContextNull() throws Exception {
        Assertions.assertThat(Utils.buildSslContext(null, null)).isNull();
    }
}
