package org.jetlinks.supports.server;

import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.*;
import org.jetlinks.core.server.MessageHandler;
import org.jetlinks.core.server.session.DeviceSessionManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import javax.annotation.Nonnull;
import java.util.function.Function;

@Slf4j
public class DefaultDecodedClientMessageHandler implements DecodedClientMessageHandler {

	private final MessageHandler messageHandler;

	private final Sinks.Many<Message> processorSinksMany;

	private final DeviceSessionManager sessionManager;

	public DefaultDecodedClientMessageHandler(MessageHandler handler, DeviceSessionManager sessionManager) {
		this(handler, sessionManager, Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false));
	}

	public DefaultDecodedClientMessageHandler(MessageHandler handler,
	                                          DeviceSessionManager sessionManager,
	                                          Sinks.Many<Message> processorSinksMany) {
		this.messageHandler = handler;
		this.processorSinksMany = processorSinksMany;
		this.sessionManager = sessionManager;
//        this.sink = processor.sink(FluxSink.OverflowStrategy.BUFFER);
	}


	protected Mono<Boolean> handleChildrenDeviceMessage(DeviceOperator device, String childrenId, Message message) {
		if (message instanceof DeviceMessageReply) {
			return doReply(((DeviceMessageReply) message));
		} else if (message instanceof DeviceOnlineMessage) {
			return sessionManager.registerChildren(device.getDeviceId(), childrenId)
					.thenReturn(true)
					.defaultIfEmpty(false);
		} else if (message instanceof DeviceOfflineMessage) {
			return sessionManager.unRegisterChildren(device.getDeviceId(), childrenId)
					.thenReturn(true)
					.defaultIfEmpty(false);
		}
		return Mono.just(true);
	}

	protected Mono<Boolean> handleChildrenDeviceMessageReply(DeviceOperator session, ChildDeviceMessage reply) {
		return handleChildrenDeviceMessage(session, reply.getChildDeviceId(), reply.getChildDeviceMessage());
	}

	protected Mono<Boolean> handleChildrenDeviceMessageReply(DeviceOperator session, ChildDeviceMessageReply reply) {
		return handleChildrenDeviceMessage(session, reply.getChildDeviceId(), reply.getChildDeviceMessage());
	}

	public void shutdown() {

	}

	public Flux<Message> subscribe() {
		return processorSinksMany
				.asFlux()
				.map(Function.identity())
				.doOnError(err -> log.error(err.getMessage(), err));
	}

	@Override
	public Mono<Boolean> handleMessage(DeviceOperator device, @Nonnull Message message) {
		return Mono
				.defer(() -> {
					if (device != null) {
						if (message instanceof ChildDeviceMessageReply) {
							return handleChildrenDeviceMessageReply(device, ((ChildDeviceMessageReply) message));
						} else if (message instanceof ChildDeviceMessage) {
							return handleChildrenDeviceMessageReply(device, ((ChildDeviceMessage) message));
						}
					}
					if (message instanceof DeviceMessageReply) {
						return doReply(((DeviceMessageReply) message));
					}
					return Mono.just(true);
				})
				.defaultIfEmpty(false)
				.doFinally(s -> {
					if (processorSinksMany.currentSubscriberCount() != 0) {
						processorSinksMany.tryEmitNext(message);
					}
				}).onErrorContinue((err, res) -> log.error("handle device[{}] message [{}] error", device.getDeviceId(), message, err));

	}

	private Mono<Boolean> doReply(DeviceMessageReply reply) {
		if (log.isDebugEnabled()) {
			log.debug("reply message {}", reply.getMessageId());
		}
		return messageHandler
				.reply(reply)
				.doOnSuccess(success -> {
					if (log.isDebugEnabled()) {
						log.debug("reply message {} complete", reply.getMessageId());
					}
				})
				.thenReturn(true)
				.doOnError((error) -> log.error("reply message error", error))
				;
	}
}
