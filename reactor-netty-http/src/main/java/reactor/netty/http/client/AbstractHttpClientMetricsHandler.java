/*
 * Copyright (c) 2021-2022 VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.http.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import reactor.netty.channel.ChannelOperations;
import reactor.util.annotation.Nullable;
import reactor.util.context.ContextView;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.Function;

/**
 * @author Violeta Georgieva
 * @since 1.0.8
 */
abstract class AbstractHttpClientMetricsHandler extends ChannelDuplexHandler {

	String path;

	String method;

	String status;

	ContextView contextView;


	long dataReceived;

	long dataSent;


	long dataReceivedTime;

	long dataSentTime;

	final Function<String, String> uriTagValue;

	protected AbstractHttpClientMetricsHandler(@Nullable Function<String, String> uriTagValue) {
		this.uriTagValue = uriTagValue;
	}

	@Override
	@SuppressWarnings("FutureReturnValueIgnored")
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
		if (msg instanceof HttpRequest) {
			method = ((HttpRequest) msg).method().name();

			ChannelOperations<?, ?> channelOps = ChannelOperations.get(ctx.channel());
			if (channelOps instanceof HttpClientOperations) {
				HttpClientOperations ops = (HttpClientOperations) channelOps;
				path = uriTagValue == null ? ops.path : uriTagValue.apply(ops.path);
				contextView = ops.currentContextView();
			}

			startWrite((HttpRequest) msg, ctx.channel().remoteAddress());
		}

		if (msg instanceof ByteBufHolder) {
			dataSent += ((ByteBufHolder) msg).content().readableBytes();
		}
		else if (msg instanceof ByteBuf) {
			dataSent += ((ByteBuf) msg).readableBytes();
		}

		if (msg instanceof LastHttpContent) {
			SocketAddress address = ctx.channel().remoteAddress();
			promise.addListener(future -> recordWrite(address));
		}
		//"FutureReturnValueIgnored" this is deliberate
		ctx.write(msg, promise);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		if (msg instanceof HttpResponse) {
			status = ((HttpResponse) msg).status().codeAsText().toString();

			startRead((HttpResponse) msg, ctx.channel().remoteAddress());
		}

		if (msg instanceof ByteBufHolder) {
			dataReceived += ((ByteBufHolder) msg).content().readableBytes();
		}
		else if (msg instanceof ByteBuf) {
			dataReceived += ((ByteBuf) msg).readableBytes();
		}

		if (msg instanceof LastHttpContent) {
			recordRead(ctx.channel().remoteAddress());
			reset();
		}

		ctx.fireChannelRead(msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		recordException(ctx);

		ctx.fireExceptionCaught(cause);
	}

	protected abstract HttpClientMetricsRecorder recorder();

	protected void recordException(ChannelHandlerContext ctx) {
		recorder().incrementErrorsCount(ctx.channel().remoteAddress(),
				path != null ? path : resolveUri(ctx));
	}

	protected void recordRead(SocketAddress address) {
		recorder().recordDataReceivedTime(address,
				path, method, status,
				Duration.ofNanos(System.nanoTime() - dataReceivedTime));

		recorder().recordResponseTime(address,
				path, method, status,
				Duration.ofNanos(System.nanoTime() - dataSentTime));

		recorder().recordDataReceived(address, path, dataReceived);
	}

	protected void recordWrite(SocketAddress address) {
		recorder().recordDataSentTime(address,
				path, method,
				Duration.ofNanos(System.nanoTime() - dataSentTime));

		recorder().recordDataSent(address, path, dataSent);
	}

	protected void reset() {
		path = null;
		method = null;
		status = null;
		contextView = null;
		dataReceived = 0;
		dataSent = 0;
		dataReceivedTime = 0;
		dataSentTime = 0;
	}

	protected void startRead(HttpResponse msg, SocketAddress address) {
		dataReceivedTime = System.nanoTime();
	}

	protected void startWrite(HttpRequest msg, SocketAddress address) {
		dataSentTime = System.nanoTime();
	}

	private String resolveUri(ChannelHandlerContext ctx) {
		ChannelOperations<?, ?> channelOps = ChannelOperations.get(ctx.channel());
		if (channelOps instanceof HttpClientOperations) {
			String path = ((HttpClientOperations) channelOps).uri();
			return uriTagValue == null ? path : uriTagValue.apply(path);
		}
		else {
			return "unknown";
		}
	}
}
