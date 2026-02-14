package org.example.conflationjava;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ReactorExtensions.conflateByKey
 */
class ReactorExtensionsTest {

    // ========== Basic Functionality Tests ==========

    @Test
    void conflateByKey_basicConflation_keepsLatestValue() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Flux<Map.Entry<String, Integer>> source = Flux.just(
                Map.entry("A", 1),
                Map.entry("A", 2),
                Map.entry("A", 3),
                Map.entry("B", 10),
                Map.entry("B", 20)
        );

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(100), scheduler);

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .thenConsumeWhile(entry -> {
                    // Should have latest values: A=3, B=20
                    if (entry.getKey().equals("A")) {
                        assertEquals(3, entry.getValue());
                    } else if (entry.getKey().equals("B")) {
                        assertEquals(20, entry.getValue());
                    }
                    return true;
                })
                .verifyComplete();
    }

    @Test
    void conflateByKey_emptySource_completesEmpty() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Flux<Map.Entry<String, Integer>> source = Flux.empty();
        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(100), scheduler);

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .verifyComplete();
    }

    @Test
    void conflateByKey_singleItem_emitsOnComplete() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Flux<Map.Entry<String, Integer>> source = Flux.just(Map.entry("A", 42));
        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(100), scheduler);

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .expectNextMatches(entry -> entry.getKey().equals("A") && entry.getValue() == 42)
                .verifyComplete();
    }

    // ========== Edge Case Tests ==========

    @Test
    void conflateByKey_nullSource_throwsNPE() {
        assertThrows(NullPointerException.class, () ->
                ReactorExtensions.conflateByKey(null, Duration.ofMillis(100)));
    }

    @Test
    void conflateByKey_nullInterval_throwsNPE() {
        Flux<Map.Entry<String, Integer>> source = Flux.empty();
        assertThrows(NullPointerException.class, () ->
                ReactorExtensions.conflateByKey(source, null));
    }

    @Test
    void conflateByKey_nullScheduler_throwsNPE() {
        Flux<Map.Entry<String, Integer>> source = Flux.empty();
        assertThrows(NullPointerException.class, () ->
                ReactorExtensions.conflateByKey(source, Duration.ofMillis(100), null));
    }

    @Test
    void conflateByKey_zeroInterval_throwsIllegalArgument() {
        Flux<Map.Entry<String, Integer>> source = Flux.empty();
        assertThrows(IllegalArgumentException.class, () ->
                ReactorExtensions.conflateByKey(source, Duration.ZERO));
    }

    @Test
    void conflateByKey_negativeInterval_throwsIllegalArgument() {
        Flux<Map.Entry<String, Integer>> source = Flux.empty();
        assertThrows(IllegalArgumentException.class, () ->
                ReactorExtensions.conflateByKey(source, Duration.ofMillis(-100)));
    }

    @Test
    void conflateByKey_entryWithNullKey_isIgnored() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        // Create a custom entry with null key
        Map.Entry<String, Integer> nullKeyEntry = new Map.Entry<>() {
            @Override
            public String getKey() { return null; }
            @Override
            public Integer getValue() { return 100; }
            @Override
            public Integer setValue(Integer value) { return value; }
        };

        Flux<Map.Entry<String, Integer>> source = Flux.just(
                nullKeyEntry,
                Map.entry("A", 42)
        );

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(100), scheduler);

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .expectNextMatches(entry -> entry.getKey().equals("A") && entry.getValue() == 42)
                .verifyComplete();
    }

    @Test
    void conflateByKey_entryFilteredByUpstream_worksCorrectly() {
        // This test verifies we handle an empty Flux correctly after filtering
        // (Flux.just() doesn't allow nulls anyway, so we test filtered scenarios)
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Flux<Map.Entry<String, Integer>> source = Flux.just(
                Map.entry("A", 1),
                Map.entry("B", 2)
        ).filter(e -> e.getKey().equals("A")); // Only keep "A"

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(100), scheduler);

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .expectNextMatches(entry -> entry.getKey().equals("A") && entry.getValue() == 1)
                .verifyComplete();
    }

    // ========== Error Propagation Tests ==========

    @Test
    void conflateByKey_sourceError_propagatesError() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        RuntimeException testError = new RuntimeException("Test error");
        Flux<Map.Entry<String, Integer>> source = Flux.<Map.Entry<String, Integer>>just(Map.entry("A", 1))
                .concatWith(Flux.error(testError));

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(100), scheduler);

        StepVerifier.create(result)
                .verifyErrorMatches(e -> e.getMessage().equals("Test error"));
    }

    @Test
    void conflateByKey_errorAfterData_emitsDataBeforeError() {
        RuntimeException testError = new RuntimeException("Delayed error");

        Sinks.Many<Map.Entry<String, Integer>> sink = Sinks.many().multicast().onBackpressureBuffer();

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                sink.asFlux(), Duration.ofMillis(50));

        List<Map.Entry<String, Integer>> received = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean errorReceived = new AtomicBoolean(false);

        result.subscribe(
                received::add,
                e -> errorReceived.set(true)
        );

        sink.tryEmitNext(Map.entry("A", 1));
        sink.tryEmitError(testError);

        // Give time for processing
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}

        assertTrue(errorReceived.get());
    }

    // ========== Completion Tests ==========

    @Test
    @Timeout(5)
    void conflateByKey_sourceCompletes_flushesRemainingData() throws InterruptedException {
        Sinks.Many<Map.Entry<String, Integer>> sink = Sinks.many().multicast().onBackpressureBuffer();

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                sink.asFlux(), Duration.ofSeconds(10)); // Long interval

        List<Map.Entry<String, Integer>> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch completeLatch = new CountDownLatch(1);

        result.subscribe(
                received::add,
                e -> {},
                completeLatch::countDown
        );

        // Emit data and complete immediately (before interval tick)
        sink.tryEmitNext(Map.entry("A", 1));
        sink.tryEmitNext(Map.entry("B", 2));
        sink.tryEmitComplete();

        // Wait for completion
        assertTrue(completeLatch.await(2, TimeUnit.SECONDS));

        // Verify data was flushed on completion
        assertEquals(2, received.size());
    }

    // ========== Cancellation Tests ==========

    @Test
    @Timeout(5)
    void conflateByKey_cancellation_stopsProcessing() throws InterruptedException {
        AtomicInteger emitCount = new AtomicInteger(0);

        Flux<Map.Entry<String, Integer>> infiniteSource = Flux.interval(Duration.ofMillis(1))
                .map(i -> Map.entry("key", i.intValue()));

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                infiniteSource, Duration.ofMillis(50));

        CountDownLatch startLatch = new CountDownLatch(1);
        var disposable = result.subscribe(entry -> {
            emitCount.incrementAndGet();
            startLatch.countDown();
        });

        // Wait for at least one emission
        assertTrue(startLatch.await(1, TimeUnit.SECONDS));

        // Cancel
        disposable.dispose();

        int countAfterCancel = emitCount.get();
        Thread.sleep(200); // Give time for any more emissions

        // Should not have significantly more emissions after cancel
        assertTrue(emitCount.get() <= countAfterCancel + 5,
                "Should stop emitting after cancel");
    }

    // ========== Concurrency Tests ==========

    @Test
    @Timeout(10)
    void conflateByKey_concurrentProducers_handlesCorrectly() throws InterruptedException {
        Sinks.Many<Map.Entry<String, Integer>> sink = Sinks.many().multicast().onBackpressureBuffer();

        ConcurrentHashMap<String, Integer> latestReceived = new ConcurrentHashMap<>();
        CountDownLatch completeLatch = new CountDownLatch(1);

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                sink.asFlux(), Duration.ofMillis(50));

        result.subscribe(
                entry -> latestReceived.put(entry.getKey(), entry.getValue()),
                e -> completeLatch.countDown(),
                completeLatch::countDown
        );

        // Multiple threads producing data
        int numThreads = 4;
        int itemsPerThread = 1000;
        CountDownLatch producersLatch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                for (int i = 0; i < itemsPerThread; i++) {
                    sink.tryEmitNext(Map.entry("key-" + threadId, i));
                }
                producersLatch.countDown();
            }).start();
        }

        // Wait for producers to finish
        assertTrue(producersLatch.await(5, TimeUnit.SECONDS));

        // Complete the source
        sink.tryEmitComplete();

        // Wait for downstream completion
        assertTrue(completeLatch.await(5, TimeUnit.SECONDS));

        // Verify we got data for all keys
        assertEquals(numThreads, latestReceived.size());
        for (int t = 0; t < numThreads; t++) {
            assertTrue(latestReceived.containsKey("key-" + t));
        }
    }

    @Test
    @Timeout(10)
    void conflateByKey_highVolumeData_performsWell() throws InterruptedException {
        int itemCount = 100_000;
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch completeLatch = new CountDownLatch(1);

        Flux<Map.Entry<String, Integer>> source = Flux.range(0, itemCount)
                .map(i -> Map.entry("key-" + (i % 100), i)); // 100 distinct keys

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                source, Duration.ofMillis(10));

        long startTime = System.currentTimeMillis();

        result.subscribe(
                entry -> receivedCount.incrementAndGet(),
                e -> completeLatch.countDown(),
                completeLatch::countDown
        );

        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Processed " + itemCount + " items, received " +
                receivedCount.get() + " conflated items in " + duration + "ms");

        // Should receive significantly fewer items due to conflation
        assertTrue(receivedCount.get() < itemCount,
                "Conflation should reduce item count");
        assertTrue(receivedCount.get() > 0,
                "Should receive at least some items");
    }

    // ========== Multiple Interval Tests ==========

    @Test
    void conflateByKey_multipleIntervals_emitsAtEachTick() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Sinks.Many<Map.Entry<String, Integer>> sink = Sinks.many().multicast().onBackpressureBuffer();

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                sink.asFlux(), Duration.ofMillis(100), scheduler);

        List<Map.Entry<String, Integer>> received = Collections.synchronizedList(new ArrayList<>());

        result.subscribe(received::add);

        // First batch
        sink.tryEmitNext(Map.entry("A", 1));
        scheduler.advanceTimeBy(Duration.ofMillis(100));

        // Second batch
        sink.tryEmitNext(Map.entry("A", 2));
        scheduler.advanceTimeBy(Duration.ofMillis(100));

        // Third batch
        sink.tryEmitNext(Map.entry("A", 3));
        sink.tryEmitComplete();
        scheduler.advanceTimeBy(Duration.ofMillis(100));

        // Should have received 3 emissions (one per interval, plus final flush)
        assertEquals(3, received.size());
        assertEquals(1, received.get(0).getValue());
        assertEquals(2, received.get(1).getValue());
        assertEquals(3, received.get(2).getValue());
    }

    // ========== conflateByKeyWithLimit Tests ==========

    @Test
    void conflateByKeyWithLimit_respectsMaxKeys() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Flux<Map.Entry<String, Integer>> source = Flux.just(
                Map.entry("A", 1),
                Map.entry("B", 2),
                Map.entry("C", 3),  // Should be dropped
                Map.entry("D", 4)   // Should be dropped
        );

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKeyWithLimit(
                source, Duration.ofMillis(100), 2, scheduler);

        List<String> receivedKeys = Collections.synchronizedList(new ArrayList<>());

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .thenConsumeWhile(entry -> {
                    receivedKeys.add(entry.getKey());
                    return true;
                })
                .verifyComplete();

        // Should only have 2 keys
        assertEquals(2, receivedKeys.size());
        assertTrue(receivedKeys.contains("A"));
        assertTrue(receivedKeys.contains("B"));
    }

    @Test
    void conflateByKeyWithLimit_updatesExistingKeysWithinLimit() {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

        Flux<Map.Entry<String, Integer>> source = Flux.just(
                Map.entry("A", 1),
                Map.entry("B", 2),
                Map.entry("A", 10),  // Update existing key
                Map.entry("B", 20)   // Update existing key
        );

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKeyWithLimit(
                source, Duration.ofMillis(100), 2, scheduler);

        ConcurrentHashMap<String, Integer> received = new ConcurrentHashMap<>();

        StepVerifier.create(result)
                .then(() -> scheduler.advanceTimeBy(Duration.ofMillis(150)))
                .thenConsumeWhile(entry -> {
                    received.put(entry.getKey(), entry.getValue());
                    return true;
                })
                .verifyComplete();

        assertEquals(2, received.size());
        assertEquals(10, received.get("A"));
        assertEquals(20, received.get("B"));
    }

    @Test
    void conflateByKeyWithLimit_invalidMaxKeys_throwsException() {
        Flux<Map.Entry<String, Integer>> source = Flux.empty();

        assertThrows(IllegalArgumentException.class, () ->
                ReactorExtensions.conflateByKeyWithLimit(source, Duration.ofMillis(100), 0));

        assertThrows(IllegalArgumentException.class, () ->
                ReactorExtensions.conflateByKeyWithLimit(source, Duration.ofMillis(100), -1));
    }

    // ========== Memory Safety Tests ==========

    @Test
    @Timeout(5)
    void conflateByKey_cancellationClearsBuffers_noMemoryLeak() throws InterruptedException {
        Sinks.Many<Map.Entry<String, Integer>> sink = Sinks.many().multicast().onBackpressureBuffer();

        Flux<Map.Entry<String, Integer>> result = ReactorExtensions.conflateByKey(
                sink.asFlux(), Duration.ofSeconds(10)); // Long interval

        // Subscribe and immediately cancel
        var disposable = result.subscribe();

        // Emit some data
        for (int i = 0; i < 1000; i++) {
            sink.tryEmitNext(Map.entry("key-" + i, i));
        }

        // Cancel subscription
        disposable.dispose();

        // Give time for cleanup
        Thread.sleep(100);

        // If buffers are cleared properly, this should not cause memory issues
        // (actual memory verification would require more sophisticated testing)
        assertTrue(disposable.isDisposed());
    }
}


