package com.influxdb.v3.client.internal;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Promise;

public class ClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

    private final Promise<FullHttpResponse> promise;

    public ClientHandler(Promise<FullHttpResponse> promise) {
        this.promise = promise;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
        if (msg instanceof HttpResponse) {
            HttpResponse response = msg;
            System.err.println("{ START CONTENT");
        }
        if (msg instanceof HttpContent) {
            HttpContent content = msg;
            System.err.print(content.content().toString(CharsetUtil.UTF_8));
            System.err.flush();

            if (content instanceof LastHttpContent) {
                System.err.println("} END OF CONTENT");
            }
        }
        System.out.println(msg);
        this.promise.trySuccess(msg.retain());

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        this.promise.tryFailure(cause);
        cause.printStackTrace();
        ctx.close();
    }

}
