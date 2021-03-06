/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.ipc.netty.http;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import io.netty.handler.codec.http.HttpHeaders;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientResponse;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mostroverkhov
 * @author smaldini
 */
public class HttpCompressionClientServerTests {

	@Test
	public void trueEnabledIncludeContentEncoding() throws Exception {

		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(true));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		HttpClient client = HttpClient.create(o -> o.compression(true)
		                                            .connectAddress(() -> address(nettyContext)));
		HttpClientResponse res =
				client.get("/test", o -> {
				         Assert.assertTrue(o.requestHeaders()
				               .contains("Accept-Encoding", "gzip", true));
				         return o;
				      })
				      .block();

		res.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void serverCompressionDefault() throws Exception {
		HttpServer server = HttpServer.create(0);

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .block();

		assertThat(resp.responseHeaders().get("content-encoding")).isNull();

		String reply = resp.receive()
		                   .asString()
		                   .blockFirst();
		Assert.assertEquals("reply", reply);

		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void serverCompressionDisabled() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(false));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .block();

		assertThat(resp.responseHeaders().get("content-encoding")).isNull();

		String reply = resp.receive()
		                   .asString()
		                   .blockFirst();
		Assert.assertEquals("reply", reply);

		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void serverCompressionAlwaysEnabled() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(true));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .block();

		assertThat(resp.responseHeaders().get("content-encoding")).isEqualTo("gzip");

		byte[] replyBuffer = resp.receive()
		                         .aggregate()
		                         .asByteArray()
		                         .block();

		assertThat(new String(replyBuffer, Charset.defaultCharset())).isNotEqualTo("reply");

		GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(replyBuffer));
		byte deflatedBuf[] = new byte[1024];
		int readable = gis.read(deflatedBuf);
		gis.close();

		assertThat(readable).isGreaterThan(0);

		String deflated = new String(deflatedBuf, 0, readable, Charset.defaultCharset());

		assertThat(deflated).isEqualTo("reply");

		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}



	@Test
	public void serverCompressionEnabledSmallResponse() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(25));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.header("content-length", "5")
				                                  .sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .block();

		//check the server didn't send the gzip header, only transfer-encoding
		HttpHeaders headers = resp.responseHeaders();
		assertThat(headers.get("conTENT-encoding")).isNull();

		//check the server sent plain text
		String reply = resp.receive()
		                   .asString()
		                   .blockFirst();
		Assert.assertEquals("reply", reply);

		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void serverCompressionPredicateTrue() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression((req, res) -> true));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .block();

		assertThat(resp.responseHeaders().get("content-encoding")).isEqualTo("gzip");

		byte[] replyBuffer = resp.receive()
		                         .aggregate()
		                         .asByteArray()
		                         .block();

		assertThat(new String(replyBuffer, Charset.defaultCharset())).isNotEqualTo("reply");

		GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(replyBuffer));
		byte deflatedBuf[] = new byte[1024];
		int readable = gis.read(deflatedBuf);
		gis.close();

		assertThat(readable).isGreaterThan(0);

		String deflated = new String(deflatedBuf, 0, readable, Charset.defaultCharset());

		assertThat(deflated).isEqualTo("reply");

		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void serverCompressionPredicateFalse() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression((req, res) -> false));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .block();

		//check the server didn't send the gzip header, only transfer-encoding
		HttpHeaders headers = resp.responseHeaders();
		assertThat(headers.get("transFER-encoding")).isEqualTo("chunked");
		assertThat(headers.get("conTENT-encoding")).isNull();

		//check the server sent plain text
		String reply = resp.receive()
		                   .asString()
		                   .blockFirst();
		Assert.assertEquals("reply", reply);

		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void serverCompressionEnabledBigResponse() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(4));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("accept-encoding", "gzip"))
				      .block();

		assertThat(resp.responseHeaders().get("content-encoding")).isEqualTo("gzip");

		byte[] replyBuffer = resp.receive()
		                         .aggregate()
		                         .asByteArray()
		                         .block();

		assertThat(new String(replyBuffer, Charset.defaultCharset())).isNotEqualTo("reply");

		GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(replyBuffer));
		byte deflatedBuf[] = new byte[1024];
		int readable = gis.read(deflatedBuf);
		gis.close();

		assertThat(readable).isGreaterThan(0);

		String deflated = new String(deflatedBuf, 0, readable, Charset.defaultCharset());

		assertThat(deflated).isEqualTo("reply");

		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void compressionServerEnabledClientDisabledIsNone() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(true));

		String serverReply = "reply";
		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just(serverReply)))
				      .block(Duration.ofSeconds(10));

		HttpClient client = HttpClient.create(o -> o.compression(false)
		                                            .connectAddress(() -> address(nettyContext)));

		HttpClientResponse resp = client.get("/test").block();

		String reply = resp.receive()
		                   .asString()
		                   .blockFirst();

		assertThat(resp.responseHeaders().get("Content-Encoding")).isNull();
		assertThat(reply).isEqualTo(serverReply);

		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}


	@Test
	public void compressionServerDefaultClientDefaultIsNone() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));

		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));

		HttpClientResponse resp =
				client.get("/test").block();

		String reply = resp.receive()
		                   .asString()
		                   .blockFirst();

		assertThat(resp.responseHeaders().get("Content-Encoding")).isNull();
		assertThat(reply).isEqualTo("reply");

		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
		            .block();
	}

	@Test
	public void compressionActivatedOnClientAddsHeader() {
		AtomicReference<String> zip = new AtomicReference<>("fail");

		HttpServer server = HttpServer.create(o -> o.port(0).compression(true));
		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Mono.just("reply")))
				      .block(Duration.ofSeconds(10));
		HttpClient client = HttpClient.create(opt -> opt.compression(true)
		                                                .connectAddress(() -> address(nettyContext)));

		HttpClientResponse resp =
				client.get("/test", req -> {
				           zip.set(req.requestHeaders().get("accept-encoding"));
				           return req;
				      })
				      .block();

		assertThat(zip.get()).isEqualTo("gzip");
		resp.dispose();
		nettyContext.dispose();
		nettyContext.onClose()
				.block();
	}

	private InetSocketAddress address(NettyContext nettyContext) {
		return nettyContext.address();
	}

	@Test
	public void testIssue282() {
		NettyContext server =
				HttpServer.create(options -> options.compression(2048)
				                                    .port(0))
				          .newHandler((req, res) -> res.sendString(Mono.just("testtesttesttesttest")))
				          .block(Duration.ofSeconds(30));

		Mono<String> response =
				HttpClient.create(server.address().getPort())
				          .get("/")
				          .flatMap(res -> res.receive().aggregate().asString());

		StepVerifier.create(response)
		            .expectNextMatches(s -> "testtesttesttesttest".equals(s))
		            .expectComplete()
		            .verify();

		server.dispose();
	}

	@Test
	public void testIssue292() {
		NettyContext server =
				HttpServer.create(options -> options.compression(10)
				                                    .port(0))
				          .newHandler((req, res) ->
				                  res.sendHeaders().sendString(Flux.just("test", "test", "test", "test")))
				          .block(Duration.ofSeconds(30));

		Mono<String> response =
				HttpClient.create(options -> options.port(server.address().getPort())
				                                    .compression(true))
				          .get("/")
				          .flatMap(res -> res.receive().aggregate().asString());

		StepVerifier.create(response)
		            .expectNextMatches(s -> "testtesttesttest".equals(s))
		            .expectComplete()
		            .verify();

		response = HttpClient.create(options -> options.port(server.address().getPort())
		                                               .compression(true))
		                     .get("/")
		                     .flatMap(res -> res.receive().aggregate().asString());

		StepVerifier.create(response)
		            .expectNextMatches(s -> "testtesttesttest".equals(s))
		            .expectComplete()
		            .verify();

		server.dispose();
	}
}
