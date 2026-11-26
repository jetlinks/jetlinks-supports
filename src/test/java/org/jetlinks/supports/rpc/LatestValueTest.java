package org.jetlinks.supports.rpc;

import org.junit.Test;
import reactor.test.StepVerifier;
import java.util.*;
import static org.junit.Assert.*;

public class LatestValueTest {
    @Test public void failedViewsNeverEmitASuccessfulSnapshotToLateSubscribers() {
        LatestValue<Integer> source = new LatestValue<>(1); RuntimeException failure = new IllegalStateException("failed view");
        StepVerifier.create(source.watch()).expectNext(1).then(() -> source.fail(failure)).expectErrorMatches(error -> error == failure).verify();
        StepVerifier.create(source.watch().next()).expectErrorMatches(error -> error == failure).verify();
    }
    @Test public void initialSnapshotAndSlowObserverConvergeWithoutBufferingAllUpdates() {
        LatestValue<Integer> source = new LatestValue<>(0);
        StepVerifier.create(source.watch(), 0)
            .then(() -> { for (int i = 1; i <= 10_000; i++) source.update(i); })
            .thenRequest(1).expectNext(10_000).thenCancel().verify();
        StepVerifier.create(source.watch().take(1)).expectNext(10_000).verifyComplete();
    }
    @Test public void reentrantUpdatesStayOrderedForAllObservers() {
        LatestValue<Integer> source = new LatestValue<>(0);
        List<Integer> first = new ArrayList<>(), second = new ArrayList<>();
        var a = source.watch().subscribe(value -> { first.add(value); if (value == 1) source.update(2); });
        var b = source.watch().subscribe(second::add);
        source.update(1);
        assertEquals(List.of(0, 1, 2), first);
        assertEquals(List.of(0, 1, 2), second);
        a.dispose(); b.dispose(); source.close();
        StepVerifier.create(source.watch()).expectNext(2).verifyComplete();
    }
}
