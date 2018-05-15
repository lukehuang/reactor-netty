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
package reactor.ipc.netty.http.client;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.ByteBufFlux;
import reactor.ipc.netty.ByteBufMono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.NettyOutbound;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.http.HttpResources;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.ipc.netty.resources.PoolResources;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import static reactor.ipc.netty.http.client.HttpClientConfiguration.*;

/**
 * An HttpClient allows to build in a safe immutable way an http client that is
 * materialized and connecting when {@link TcpClient#connect)} is ultimately called.
 * <p> Internally, materialization happens in three phases, first {@link #tcpConfiguration()}
 * is called to retrieve a ready to use {@link TcpClient}, then {@link
 * TcpClient#configure()} retrieve a usable {@link Bootstrap} for the final {@link
 * TcpClient#connect)} is called.
 * <p> Examples:
 * <pre>
 * {@code
 * HttpClient.create("http://example.com")
 *           .get()
 *           .response()
 *           .block();
 * }
 * {@code
 * HttpClient.create("http://example.com")
 *           .post()
 *           .send(Flux.just(bb1, bb2, bb3))
 *           .responseSingle((res, content) -> Mono.just(res.status().code()))
 *           .block();
 * }
 * {@code
 * HttpClient.prepare()
 *           .baseUri("http://example.com")
 *           .post()
 *           .send(ByteBufFlux.fromString(flux))
 *           .responseSingle((res, content) -> Mono.just(res.status().code()))
 *           .block();
 * }
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
public abstract class HttpClient {

	public static final String                         USER_AGENT =
			String.format("ReactorNetty/%s", reactorNettyVersion());

	/**
	 * A URI configuration
	 */
	public interface UriConfiguration<S extends ResponseReceiver<?>> {

		/**
		 * Configure URI to use for this request/response
		 *
		 * @param uri target URI, if starting with "/" it will be prepended with
		 * {@link #baseUrl(String)} when available
		 *
		 * @return the appropriate sending or receiving contract
		 */
		S uri(String uri);

		/**
		 * Configure URI to use for this request/response on subscribe
		 *
		 * @param uri target URI, if starting with "/" it will be prepended with
		 * {@link #baseUrl(String)} when available
		 *
		 * @return the appropriate sending or receiving contract
		 */
		S uri(Mono<String> uri);
	}

	/**
	 * Allow a request body configuration before calling one of the terminal, {@link
	 * Publisher} based, {@link ResponseReceiver} API.
	 */
	public interface RequestSender extends ResponseReceiver<RequestSender> {

		/**
		 * Configure a body to send on request.
		 *
		 * @param body  a body publisher that will terminate the request on complete
		 *
		 * @return a new {@link ResponseReceiver}
		 */
		ResponseReceiver<?> send(Publisher<? extends ByteBuf> body);

		/**
		 * Configure a body to send on request using the {@link NettyOutbound} sending
		 * builder and returning a {@link Publisher} to signal end of the request.
		 *
		 * @param sender a bifunction given the outgoing request and the sending
		 * {@link NettyOutbound}, returns a publisher that will terminate the request
		 * body
		 * on complete
		 *
		 * @return a new {@link ResponseReceiver}
		 */
		ResponseReceiver<?> send(BiFunction<? super HttpClientRequest, ? super NettyOutbound, ? extends Publisher<Void>> sender);
	}

	/**
	 * A response extractor for this configured {@link HttpClient}. Since
	 * {@link ResponseReceiver} API returns {@link Flux} or {@link Mono},
	 * requesting is always deferred to {@link Publisher#subscribe(Subscriber)}.
	 */
	public interface ResponseReceiver<S extends ResponseReceiver<?>>
			extends UriConfiguration<S> {

		/**
		 * Return the response status and headers as {@link HttpClientResponse}
		 * <p> Will automatically close the response if necessary.
		 *
		 * @return the response status and headers as {@link HttpClientResponse}
		 */
		Mono<HttpClientResponse> response();

		/**
		 * Extract a response flux from the given {@link HttpClientResponse} and body
		 * {@link ByteBufFlux}.
		 * <p> Will automatically close the response if necessary after the returned
		 * {@link Flux} terminates or is being cancelled.
		 *
		 * @param receiver extracting receiver
		 * @param <V> the extracted flux type
		 *
		 * @return a {@link Flux} forwarding the returned {@link Publisher} sequence
		 */
		<V> Flux<V> response(BiFunction<? super HttpClientResponse, ? super ByteBufFlux, ? extends Publisher<V>> receiver);

		/**
		 * Extract a response flux from the given {@link HttpClientResponse} and
		 * underlying {@link Connection}.
		 * <p> The connection will not automatically {@link Connection#dispose()} and
		 * manual interaction with this method might be necessary if the remote never
		 * terminates itself.
		 *
		 * @param receiver extracting receiver
		 * @param <V> the extracted flux type
		 *
		 * @return a {@link Flux} forwarding the returned {@link Publisher} sequence
		 */
		<V> Flux<V> responseConnection(BiFunction<? super HttpClientResponse, ? super Connection, ? extends Publisher<V>> receiver);

		/**
		 * Return the response body chunks as {@link ByteBufFlux}.
		 *
		 * <p> Will automatically close the response if necessary after the returned
		 * {@link Flux} terminates or is being cancelled.
		 *
		 * @return the response body chunks as {@link ByteBufFlux}.
		 */
		ByteBufFlux responseContent();

		/**
		 * Extract a response mono from the given {@link HttpClientResponse} and
		 * aggregated body {@link ByteBufMono}.
		 *
		 * <p> Will automatically close the response if necessary after the returned
		 * {@link Mono} terminates or is being cancelled.
		 *
		 * @param receiver extracting receiver
		 * @param <V> the extracted mono type
		 *
		 * @return a {@link Mono} forwarding the returned {@link Mono} result
		 */
		<V> Mono<V> responseSingle(BiFunction<? super HttpClientResponse, ? super ByteBufMono, ? extends Mono<V>> receiver);

		/**
		 * Negotiate a websocket upgrade and extract a flux from the given
		 * {@link reactor.ipc.netty.http.websocket.WebsocketInbound} and
		 *  {@link reactor.ipc.netty.http.websocket.WebsocketOutbound}}.
		 * <p> The connection will not automatically {@link Connection#dispose()} and
		 * manual disposing with the {@link Connection} or returned {@link Flux} might be
		 * necessary if the remote never
		 * terminates itself.
		 *
		 * @param receiver extracting receiver
		 * @param <V> the extracted flux type
		 *
		 * @return a {@link RequestSender} ready to consume for response
		 */
		default <V> Flux<V> websocket(BiFunction<? super WebsocketInbound, ? super WebsocketOutbound, ? extends Publisher<V>> receiver) {
			return websocket("", receiver);
		}

		/**
		 * Negotiate a websocket upgrade and extract a flux from the given
		 * {@link reactor.ipc.netty.http.websocket.WebsocketInbound} and
		 *  {@link reactor.ipc.netty.http.websocket.WebsocketOutbound}}.
		 * <p> The connection will not automatically {@link Connection#dispose()} and
		 * manual disposing with the {@link Connection} or returned {@link Flux} might be
		 * necessary if the remote never terminates itself.
		 *
		 * @param subprotocols the subprotocol(s) to negotiate, comma-separated, or null if
		 * not relevant.
		 * @param receiver extracting receiver
		 * @param <V> the extracted flux type
		 *
		 * @return a {@link RequestSender} ready to consume for response
		 */
		<V> Flux<V> websocket(String subprotocols, BiFunction<? super WebsocketInbound, ? super WebsocketOutbound, ? extends Publisher<V>> receiver);
	}



	/**
	 * Prepare a pooled {@link HttpClient} given the base URI.
	 *
	 * @return a new {@link HttpClient}
	 */
	public static HttpClient create(String baseUri) {
		return prepare().baseUrl(baseUri);
	}

	/**
	 * Prepare an unpooled {@link HttpClient}. {@link UriConfiguration#uri(String)} or
	 * {@link #baseUrl(String)} should be invoked after a verb
	 * {@link #request(HttpMethod)} is selected.
	 *
	 * @return a {@link HttpClient}
	 */
	public static HttpClient newConnection() {
		return HttpClientConnect.INSTANCE;
	}

	/**
	 * Prepare a pooled {@link HttpClient}. {@link UriConfiguration#uri(String)} or
	 * {@link #baseUrl(String)} should be invoked before a verb
	 * {@link #request(HttpMethod)} is selected.
	 *
	 * @return a {@link HttpClient}
	 */
	public static HttpClient prepare() {
		return prepare(HttpResources.get());
	}

	/**
	 * Prepare a pooled {@link HttpClient}. {@link UriConfiguration#uri(String)} or
	 * {@link #baseUrl(String)} should be invoked before a verb
	 * {@link #request(HttpMethod)} is selected.
	 *
	 * @return a {@link HttpClient}
	 */
	public static HttpClient prepare(PoolResources poolResources) {
		return new HttpClientConnect(TcpClient.create(poolResources)
		                                      .bootstrap(HTTP_OPS_CONF)
		                                      .port(80));
	}

	/**
	 * Prepare a pooled {@link HttpClient}
	 *
	 * @return a {@link HttpClient}
	 */
	public static HttpClient from(TcpClient tcpClient) {
		return new HttpClientConnect(tcpClient.bootstrap(HTTP_OPS_CONF));
	}

	/**
	 * Configure URI to use for this request/response
	 *
	 * @param baseUrl a default base url that can be fully sufficient for request or can
	 * be used to prepend future {@link UriConfiguration#uri} calls.
	 *
	 * @return the appropriate sending or receiving contract
	 */
	public final HttpClient baseUrl(String baseUrl) {
		Objects.requireNonNull(baseUrl, "baseUrl");
		return tcpConfiguration(tcp -> tcp.bootstrap(b -> HttpClientConfiguration.baseUrl(b, baseUrl)));
	}

	/**
	 * The address to which this client should connect for each subscribe.
	 *
	 * @param connectAddressSupplier A supplier of the address to connect to.
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient addressSupplier(Supplier<? extends SocketAddress> connectAddressSupplier) {
		Objects.requireNonNull(connectAddressSupplier, "connectAddressSupplier");
		return tcpConfiguration(tcpClient -> tcpClient.addressSupplier(connectAddressSupplier));
	}

	/**
	 * Enable gzip compression
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient compress() {
		return tcpConfiguration(COMPRESS_ATTR_CONFIG).headers(COMPRESS_HEADERS);
	}

	/**
	 * Enable http status 301/302 auto-redirect support
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient followRedirect() {
		return tcpConfiguration(FOLLOW_REDIRECT_ATTR_CONFIG);
	}

	/**
	 * Enable transfer-encoding
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient chunkedTransfer() {
		return tcpConfiguration(CHUNKED_ATTR_CONFIG);
	}

	/**
	 * HTTP DELETE to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to prepare the content for response
	 */
	public final RequestSender delete() {
		return request(HttpMethod.DELETE);
	}

	/**
	 * Setup a callback called when {@link HttpClientRequest} is about to be sent.
	 *
	 * @param doOnRequest a consumer observing connected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doOnRequest(BiConsumer<? super HttpClientRequest, ? super Connection> doOnRequest) {
		return new HttpClientLifecycle(this, doOnRequest, null, null, null);
	}

	/**
	 * Setup a callback called when {@link HttpClientRequest} has been sent
	 *
	 * @param doAfterRequest a consumer observing connected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doAfterRequest(BiConsumer<? super HttpClientRequest, ? super Connection> doAfterRequest) {
		return new HttpClientLifecycle(this, null, doAfterRequest, null, null);
	}

	/**
	 * Setup a callback called after {@link HttpClientResponse} headers have been
	 * received
	 *
	 * @param doOnResponse a consumer observing connected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doOnResponse(BiConsumer<? super HttpClientResponse, ? super Connection> doOnResponse) {
		return new HttpClientLifecycle(this, null, null, doOnResponse, null);
	}

	/**
	 * Setup a callback called after {@link HttpClientResponse} has been fully received.
	 *
	 * @param doAfterResponse a consumer observing disconnected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doAfterResponse(BiConsumer<? super HttpClientResponse, ? super Connection> doAfterResponse) {
		return new HttpClientLifecycle(this, null, null, null, doAfterResponse);
	}

	/**
	 * HTTP GET to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public final ResponseReceiver<?> get() {
		return request(HttpMethod.GET);
	}

	/**
	 * HTTP GET to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public final ResponseReceiver<?> head() {
		return request(HttpMethod.HEAD);
	}

	/**
	 * Apply headers configuration.
	 *
	 * @param headerBuilder the  header {@link Consumer} to invoke before sending
	 * websocket handshake
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient headers(Consumer<? super HttpHeaders> headerBuilder) {
		return new HttpClientHeaders(this, headerBuilder);
	}

	/**
	 * Disable gzip compression
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient noCompression() {
		return tcpConfiguration(COMPRESS_ATTR_DISABLE).headers(COMPRESS_HEADERS_DISABLE);
	}

	/**
	 * Disable http status 301/302 auto-redirect support
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient noRedirection() {
		return tcpConfiguration(FOLLOW_REDIRECT_ATTR_DISABLE);
	}

	/**
	 * Disable transfer-encoding
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient noChunkedTransfer() {
		return tcpConfiguration(CHUNKED_ATTR_DISABLE);
	}

	/**
	 * HTTP OPTIONS to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public final ResponseReceiver<?> options() {
		return request(HttpMethod.OPTIONS);
	}

	/**
	 * HTTP PATCH to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public final RequestSender patch() {
		return request(HttpMethod.PATCH);
	}

	/**
	 * HTTP POST to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public final RequestSender post() {
		return request(HttpMethod.POST);
	}

	/**
	 * HTTP PUT to connect the {@link HttpClient}.
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public final RequestSender put() {
		return request(HttpMethod.PUT);
	}

	/**
	 * Use the passed HTTP method to connect the {@link HttpClient}.
	 *
	 * @param method the HTTP method to send
	 *
	 * @return a {@link RequestSender} ready to consume for response
	 */
	public RequestSender request(HttpMethod method) {
		Objects.requireNonNull(method, "method");
		TcpClient tcpConfiguration = tcpConfiguration().bootstrap(b -> method(b, method));
		return new HttpClientFinalizer(tcpConfiguration);
	}

	/**
	 * Apply {@link Bootstrap} configuration given mapper taking currently configured one
	 * and returning a new one to be ultimately used for socket binding. <p> Configuration
	 * will apply during {@link #tcpConfiguration()} phase.
	 *
	 * @param tcpMapper A tcpClient mapping function to update tcp configuration and
	 * return an enriched tcp client to use.
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient tcpConfiguration(Function<? super TcpClient, ? extends TcpClient> tcpMapper) {
		return new HttpClientTcpConfig(this, tcpMapper);
	}

	/**
	 * The port to which this client should connect.
	 *
	 * @param port The port to connect to.
	 *
	 * @return a new {@link TcpClient}
	 */
	public final HttpClient port(int port) {
		return tcpConfiguration(tcpClient -> tcpClient.port(port));
	}

	/**
	 * Apply a wire logger configuration using {@link TcpServer} category
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpClient wiretap() {
		return tcpConfiguration(tcpClient -> tcpClient.bootstrap(b -> BootstrapHandlers.updateLogSupport(b, LOGGING_HANDLER)));
	}

	/**
	 * Get a TcpClient from the parent {@link HttpClient} chain to use with {@link
	 * TcpClient#connect()}} or separately
	 *
	 * @return a configured {@link TcpClient}
	 */
	protected TcpClient tcpConfiguration() {
		return DEFAULT_TCP_CLIENT;
	}


	static final ChannelOperations.OnSetup HTTP_OPS =
			(ch, c, msg) -> new HttpClientOperations(ch, c).bind();

	static final Function<Bootstrap, Bootstrap> HTTP_OPS_CONF = b -> {
		BootstrapHandlers.channelOperationFactory(b, HTTP_OPS);
		return b;
	};

	static String reactorNettyVersion() {
		return Optional.ofNullable(HttpClient.class.getPackage()
		                                           .getImplementationVersion())
		               .orElse("dev");
	}

	final static String                    WS_SCHEME    = "ws";
	final static String                    WSS_SCHEME   = "wss";
	final static String                    HTTP_SCHEME  = "http";
	final static String                    HTTPS_SCHEME = "https";

	static final TcpClient DEFAULT_TCP_CLIENT = TcpClient.newConnection()
	                                                     .bootstrap(HTTP_OPS_CONF)
	                                                     .port(80);

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(HttpClient.class);

	static final Function<TcpClient, TcpClient> COMPRESS_ATTR_CONFIG         =
			tcp -> tcp.bootstrap(MAP_COMPRESS);

	static final Function<TcpClient, TcpClient> COMPRESS_ATTR_DISABLE        =
			tcp -> tcp.bootstrap(MAP_NO_COMPRESS);

	static final Function<TcpClient, TcpClient> CHUNKED_ATTR_CONFIG          =
			tcp -> tcp.bootstrap(MAP_CHUNKED);

	static final Function<TcpClient, TcpClient> CHUNKED_ATTR_DISABLE         =
			tcp -> tcp.bootstrap(MAP_NO_CHUNKED);

	static final Function<TcpClient, TcpClient> FOLLOW_REDIRECT_ATTR_CONFIG  =
			tcp -> tcp.bootstrap(MAP_REDIRECT);

	static final Function<TcpClient, TcpClient> FOLLOW_REDIRECT_ATTR_DISABLE =
			tcp -> tcp.bootstrap(MAP_NO_REDIRECT);

	static final Consumer<? super HttpHeaders> COMPRESS_HEADERS = h ->
			h.add(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

	static final Consumer<? super HttpHeaders> COMPRESS_HEADERS_DISABLE = h -> {
		if (isCompressing(h)) {
			h.remove(HttpHeaderNames.ACCEPT_ENCODING);
		}
	};

	static boolean isCompressing(HttpHeaders h){
		return h.contains(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP, true);
	}
}
