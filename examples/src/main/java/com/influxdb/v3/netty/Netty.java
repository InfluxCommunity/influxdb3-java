package com.influxdb.v3.netty;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.config.ClientConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import javax.net.ssl.SSLException;

import java.util.UUID;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class Netty {

    public static void main(String[] args) throws InterruptedException, SSLException {
//        ClientConfig clientConfig = configLocal();
        ClientConfig clientConfig = configCloud();

        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);

        // Version
//        System.out.println("Server version: " + influxDBClient.getServerVersion());

        // Write
//        var testId = UUID.randomUUID().toString();
//        var p = Point.measurement("cpu_sonnh")
//                .setTag("host", "server1")
//                .setField("usage_idle", 90.0f)
//                .setField("testId", testId);
//        System.out.println("testId: " + testId + "");
//        influxDBClient.writePoint(p);


        System.out.println("--------------------");

        NettyClient nettyClient = new NettyClient(clientConfig);

    }

    public static ClientConfig configCloud() {
        String url = System.getenv("TESTING_INFLUXDB_URL");
        String token = System.getenv("TESTING_INFLUXDB_TOKEN");
        String database = System.getenv("TESTING_INFLUXDB_DATABASE");
        return new ClientConfig.Builder()
                .host(url)
                .token(token.toCharArray())
                .database(database)
                .build();
    }

    public static ClientConfig configLocal() {
        String url = "localhost";
        String token = "apiv3_sMYBS-vRxl6UDMylb7m2u64G6R7g61jlGL76XnUJY3EaN4MD0tZd4DZOBhe6j-dYtoVhrC6PqGgI9Xiv8d3Psw";
        String database = System.getenv("bucket0");
        return new ClientConfig.Builder()
                .host(url)
                .token(token.toCharArray())
                .database(database)
                .build();
    }

    public void test() throws InterruptedException {
        System.out.println("Netty");
        var port = 8080;
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new HttpServerCodec(),
                                    new HttpContentCompressor((CompressionOptions[]) null),
                                    new HttpServerExpectContinueHandler(),
                                    new FirstHandler()

                            );
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128);

            b.bind(port)
                    .sync()
                    .channel()
                    .closeFuture()
                    .sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}


class FirstHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        final byte[] CONTENT = {'H', 'e', 'l', 'l', 'o', '1'};

        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(CONTENT));
            response.headers()
                    .set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());


            ChannelFuture f = ctx.writeAndFlush(response);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        cause.printStackTrace();
        ctx.close();
        ctx.fireExceptionCaught(cause);
    }

}