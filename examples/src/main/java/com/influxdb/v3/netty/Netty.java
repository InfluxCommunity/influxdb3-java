package com.influxdb.v3.netty;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

public class Netty {

    public static void main(String[] args) throws InterruptedException, UnrecoverableKeyException, CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        var absolutePath = "/Users/home/Documents/sources/influxdb3-java/examples/src/main/java/com/influxdb/v3/netty";
        var password = "123456";
        var format = "PKCS12";
        var host = "localhost";
        var port = 8080;

        // Start a server in another thread
        final String keyFilePath = absolutePath + "/server/pkcs12/keystore.p12";
        final String trustFilePath = absolutePath + "/server/pkcs12/truststore.p12";
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                SslContext sslContext = createSslContext(true, format, password, keyFilePath, trustFilePath, false);
                startServer(host, port, sslContext);
            } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | IOException |
                     CertificateException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(1000);

        // Create a client and call /ping
        final String clientKeyFilePath = absolutePath + "/client/pkcs12/keystore.p12";
        final String clientTrustFilePath = absolutePath + "/client/pkcs12/truststore.p12";
        final boolean isDisableKeystore = false; // Set this to "true" will throw an error because this is mTLS, so the client must also send a certificate to the server.
        SslContext sslContext = createSslContext(false, format, password, clientKeyFilePath, clientTrustFilePath, isDisableKeystore);
        HttpClient client = createClient(host, port, sslContext);
        var content = client.get().uri("/ping")
                .responseContent()
                .aggregate()
                .asString()
                .block();
        System.out.println(content);
    }

    private static HttpClient createClient(String host, int port, SslContext sslContext) {
        return HttpClient.create()
                .host(host)
                .port(port)
                .secure(sslProviderBuilder -> sslProviderBuilder.sslContext(sslContext));
    }

    private static void startServer(String host, int port, SslContext sslContext) throws UnrecoverableKeyException, CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, InterruptedException {
        BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler = (req, resp) -> resp.sendString(Mono.just("pong"));
        HttpServer.create()
                .host(host)
                .port(port)
                .secure(sslProviderBuilder -> sslProviderBuilder.sslContext(sslContext))
                .route(routes -> routes.get("/ping", handler))
                .bindNow()
                .onDispose()
                .block();
    }

    private static SslContext createSslContext(boolean isServer, String format, String password, String keyFilePath, String trustFilePath, boolean isDisableKeystore)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance(format);
        keyStore.load(new FileInputStream(keyFilePath), password.toCharArray());

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password.toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance(format);
        trustStore.load(new FileInputStream(trustFilePath), password.toCharArray());
        trustManagerFactory.init(trustStore);

        SslContextBuilder sslContextBuilder;
        if (isServer) {
            sslContextBuilder = SslContextBuilder.forServer(keyManagerFactory).clientAuth(ClientAuth.REQUIRE);
        } else {
            sslContextBuilder = SslContextBuilder.forClient();
        }

        sslContextBuilder.trustManager(trustManagerFactory);

        if (!isDisableKeystore) {
            sslContextBuilder.keyManager(keyManagerFactory);
        }

        return sslContextBuilder.build();
    }

}
