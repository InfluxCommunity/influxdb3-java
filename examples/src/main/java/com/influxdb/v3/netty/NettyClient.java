package com.influxdb.v3.netty;

import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.config.ClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.CharsetUtil;

import javax.net.ssl.SSLException;
import java.util.UUID;

public class NettyClient {

    public NettyClient(ClientConfig config) throws InterruptedException, SSLException {
        SslContext sslCtx = SslContextBuilder.forClient().build();

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast("ssl", sslCtx.newHandler(ch.alloc(), "us-east-1-1.aws.cloud2.influxdata.com", 443));
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(1048576),
                                    new CHander()
                            );
                        }
                    });

            // How to handler host + port properly
//            URI uri = new URI(config.getHost());
            ChannelFuture f = b.connect("us-east-1-1.aws.cloud2.influxdata.com", 443).sync();
            Channel ch = f.channel();


            var testId = UUID.randomUUID().toString();
            System.out.println("testId: " + testId + "");

            var p = Point.measurement("cpu_sonnh")
                    .setTag("host", "server1")
                    .setField("usage_idle", 90.0f)
                    .setField("testId", testId);
            var txt = p.toLineProtocol();
            ByteBuf content = Unpooled.copiedBuffer(txt, CharsetUtil.UTF_8);

            var writePath = "/api/v2/write";
            var pingPath = "/ping";
            QueryStringEncoder encoder = new QueryStringEncoder(writePath);
            encoder.addParam("bucket", "bucket0");
            encoder.addParam("precision", "ns");
            String uriWithParams = encoder.toString();

            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uriWithParams, content);
            request.headers().set(HttpHeaderNames.HOST, "us-east-1-1.aws.cloud2.influxdata.com");
            request.headers().set(HttpHeaderNames.AUTHORIZATION, "Token lDAtMRmhnLp5GjWNVBsieufUb66XZAPxvX3etlmi9wmeq7ispWoL06mwnxmY_BtHKoBhG4lR-c7WfrFgUXy15w==");
//            request.headers().set(HttpHeaderNames.CONNECTION, "close");
//            request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, "br, deflate, gzip, x-gzip");
            request.headers().set(HttpHeaderNames.ACCEPT, "*/*");
            request.headers().set(HttpHeaderNames.USER_AGENT, "influxdb3-java/unknown");
            request.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            request.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

            ch.writeAndFlush(request);

            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

}

class CHander extends SimpleChannelInboundHandler<FullHttpResponse> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        System.out.println(msg);
    }
}

class Out extends ChannelOutboundHandlerAdapter {

}
