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
package com.influxdb.v3.client;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

public final class TestUtils {

    private TestUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static FlightServer simpleFlightServer(@Nonnull final URI uri,
                                                  @Nonnull final BufferAllocator allocator,
                                                  @Nonnull final NoOpFlightProducer producer) throws Exception {
        Location location = Location.forGrpcInsecure(uri.getHost(), uri.getPort());
        return FlightServer.builder(allocator, location, producer).build();
    }

    public static NoOpFlightProducer simpleProducer(@Nonnull final VectorSchemaRoot vectorSchemaRoot) {
        return new NoOpFlightProducer() {
            @Override
            public void getStream(final CallContext context,
                                  final Ticket ticket,
                                  final ServerStreamListener listener) {
                listener.start(vectorSchemaRoot);
                if (listener.isReady()) {
                    listener.putNext();
                }
                listener.completed();
            }
        };
    }

    public static VectorSchemaRoot generateVectorSchemaRoot(final int fieldCount, final int rowCount) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            Field field = new Field("field" + i, FieldType.nullable(new ArrowType.Utf8()), null);
            fields.add(field);
        }

        Schema schema = new Schema(fields);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator(Long.MAX_VALUE));
        for (Field field : fields) {
            VarCharVector vector = (VarCharVector) vectorSchemaRoot.getVector(field);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                vector.set(i, "Value".getBytes(StandardCharsets.UTF_8));
            }
        }
        vectorSchemaRoot.setRowCount(rowCount);

        return vectorSchemaRoot;
    }


    // fixme
    // Create SslContext for mTLS only
    public static SslContext createNettySslContext(boolean isServer, String format, String password, String keyFilePath, String trustFilePath, boolean isDisableKeystore, boolean isJdkSslContext)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyManagerFactory keyManagerFactory = getKeyManagerFactory(format, password, keyFilePath);

        TrustManagerFactory trustManagerFactory = getTrustManagerFactory(format, password, trustFilePath);

        SslContextBuilder sslContextBuilder;
        if (isServer) {
            sslContextBuilder = SslContextBuilder.forServer(keyManagerFactory).clientAuth(ClientAuth.REQUIRE);
        } else {
            sslContextBuilder = SslContextBuilder.forClient();
        }

        sslContextBuilder.trustManager(trustManagerFactory);

        if (isJdkSslContext) {
            sslContextBuilder.sslProvider(SslProvider.JDK);
        }

        if (!isDisableKeystore) {
            sslContextBuilder.keyManager(keyManagerFactory);
        }

        return sslContextBuilder.build();
    }

    @NotNull
    private static TrustManagerFactory getTrustManagerFactory(String format, String password, String trustFilePath) throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance(format);
        trustStore.load(new FileInputStream(trustFilePath), password.toCharArray());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory;
    }

    @NotNull
    private static KeyManagerFactory getKeyManagerFactory(String format, String password, String keyFilePath) throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance(format);
        keyStore.load(new FileInputStream(keyFilePath), password.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password.toCharArray());
        return keyManagerFactory;
    }
}

