package org.jetlinks.supports.scalecube;

import io.opentelemetry.api.OpenTelemetry;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.jetlinks.core.trace.TraceHolder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExtendedClusterImplTest {

    @Test
    public void shouldUseDecoratedMessageForRequestResponseByAddressWhenTraceEnabled() {
        TraceHolder.setup(OpenTelemetry.noop());

        ClusterImpl real = mockClusterImpl();
        Address address = Address.create("127.0.0.1", 18080);
        Message request = Message.withData("test").qualifier("/test").build();

        when(real.member(address)).thenReturn(Optional.of(mock(Member.class)));
        when(real.requestResponse(eq(address), any(Message.class))).thenReturn(Mono.just(request));

        ExtendedClusterImpl cluster = new ExtendedClusterImpl(real);

        cluster.requestResponse(address, request).block();

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(real).requestResponse(eq(address), captor.capture());
        Assert.assertNotSame(request, captor.getValue());
    }

    @Test
    public void shouldUseDecoratedMessageForRequestResponseByMemberWhenTraceEnabled() {
        TraceHolder.setup(OpenTelemetry.noop());

        ClusterImpl real = mockClusterImpl();
        Member member = mock(Member.class);
        Address address = Address.create("127.0.0.1", 18081);
        Message request = Message.withData("test").qualifier("/test").build();

        when(member.address()).thenReturn(address);
        when(real.member(address)).thenReturn(Optional.of(member));
        when(real.requestResponse(eq(member), any(Message.class))).thenReturn(Mono.just(request));

        ExtendedClusterImpl cluster = new ExtendedClusterImpl(real);

        cluster.requestResponse(member, request).block();

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(real).requestResponse(eq(member), captor.capture());
        Assert.assertNotSame(request, captor.getValue());
    }

    @Test
    public void shouldUseDecoratedMessageForSpreadGossipWhenTraceEnabled() {
        TraceHolder.setup(OpenTelemetry.noop());

        ClusterImpl real = mockClusterImpl();
        Message message = Message.withData("test").qualifier("/test").build();

        when(real.spreadGossip(any(Message.class))).thenReturn(Mono.just("ok"));

        ExtendedClusterImpl cluster = new ExtendedClusterImpl(real);

        cluster.spreadGossip(message).block();

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(real).spreadGossip(captor.capture());
        Assert.assertNotSame(message, captor.getValue());
    }

    @Test
    public void shouldDeferUpdateMetadataUntilStartAndReturnCompletableMono() {
        ClusterImpl real = mockClusterImpl();
        AtomicInteger updateCalls = new AtomicInteger();

        when(real.start()).thenReturn(Mono.just(real));
        when(real.updateMetadata("metadata"))
            .thenReturn(Mono.fromRunnable(updateCalls::incrementAndGet));

        ExtendedClusterImpl cluster = new ExtendedClusterImpl(real);

        Mono<Void> pending = cluster.updateMetadata("metadata");

        Assert.assertNotNull(pending);
        Assert.assertEquals(0, updateCalls.get());

        cluster.start().block();

        StepVerifier.create(pending).verifyComplete();
        Assert.assertEquals(1, updateCalls.get());
    }

    private ClusterImpl mockClusterImpl() {
        ClusterImpl real = mock(ClusterImpl.class);
        when(real.handler(any())).thenReturn(real);
        return real;
    }
}
