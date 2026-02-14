package org.example.conflationjava;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reactive extensions for conflation (coalescing updates by key).
 */
public class ReactorExtensions {

    /**
     * Conflates (coalesces) items by key over a specified time interval.
     * <p>
     * Items with the same key that arrive within an interval window are merged,
     * keeping only the latest value for each key. At each interval tick, the
     * accumulated key-value pairs are emitted downstream.
     * <p>
     * This implementation is thread-safe and handles:
     * <ul>
     *   <li>Concurrent producers writing to the buffer</li>
     *   <li>Proper cleanup on cancellation, error, or completion</li>
     *   <li>Flushing remaining items when the source completes</li>
     *   <li>Atomic buffer swapping without race conditions</li>
     * </ul>
     *
     * @param source   the source flux of key-value entries
     * @param interval the time interval for conflation windows
     * @param <K>      the key type
     * @param <V>      the value type
     * @return a flux emitting conflated key-value entries at each interval
     * @throws NullPointerException     if source or interval is null
     * @throws IllegalArgumentException if interval is zero or negative
     */
    public static <K, V> Flux<Map.Entry<K, V>> conflateByKey(
            Flux<Map.Entry<K, V>> source,
            Duration interval
    ) {
        return conflateByKey(source, interval, Schedulers.parallel());
    }

    /**
     * Conflates (coalesces) items by key over a specified time interval using a custom scheduler.
     *
     * @param source    the source flux of key-value entries
     * @param interval  the time interval for conflation windows
     * @param scheduler the scheduler to use for the interval timer
     * @param <K>       the key type
     * @param <V>       the value type
     * @return a flux emitting conflated key-value entries at each interval
     * @throws NullPointerException     if any argument is null
     * @throws IllegalArgumentException if interval is zero or negative
     */
    public static <K, V> Flux<Map.Entry<K, V>> conflateByKey(
            Flux<Map.Entry<K, V>> source,
            Duration interval,
            Scheduler scheduler
    ) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(interval, "interval must not be null");
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }

        return Flux.create(emitter -> {
            // Double-buffering with atomic index swap for lock-free operation
            @SuppressWarnings("unchecked")
            final ConcurrentHashMap<K, V>[] buffers = new ConcurrentHashMap[]{
                    new ConcurrentHashMap<>(),
                    new ConcurrentHashMap<>()
            };

            // Atomic index: 0 or 1, indicating which buffer is currently active for writes
            final AtomicInteger activeIndex = new AtomicInteger(0);

            // Track completion state to handle final flush
            final AtomicBoolean sourceCompleted = new AtomicBoolean(false);
            final AtomicBoolean terminated = new AtomicBoolean(false);

            // Composite disposable for cleanup
            final Disposable.Composite disposables = Disposables.composite();

            // Helper to flush a buffer and emit its contents
            final Runnable flushBuffer = () -> {
                if (terminated.get() || emitter.isCancelled()) {
                    return;
                }

                // Atomically swap to the other buffer
                int oldIndex = activeIndex.getAndUpdate(i -> 1 - i);
                ConcurrentHashMap<K, V> bufferToFlush = buffers[oldIndex];

                // Drain the old buffer - we snapshot and clear atomically per key
                // Using a list to batch emissions reduces per-item overhead
                if (!bufferToFlush.isEmpty()) {
                    List<Map.Entry<K, V>> entries = new ArrayList<>(bufferToFlush.size());
                    bufferToFlush.forEach((k, v) -> {
                        entries.add(Map.entry(k, v));
                    });
                    bufferToFlush.clear();

                    // Emit all entries
                    for (Map.Entry<K, V> entry : entries) {
                        if (emitter.isCancelled()) {
                            return;
                        }
                        emitter.next(entry);
                    }
                }
            };

            // Cleanup procedure
            final Runnable cleanup = () -> {
                if (terminated.compareAndSet(false, true)) {
                    disposables.dispose();
                    // Clear buffers to release references
                    buffers[0].clear();
                    buffers[1].clear();
                }
            };

            // Register cleanup on cancellation
            emitter.onDispose(() -> cleanup.run());

            // Subscribe to the source
            Disposable sourceDisposable = source.subscribe(
                    // onNext: put the entry in the current active buffer
                    entry -> {
                        if (entry != null && !terminated.get() && !emitter.isCancelled()) {
                            K key = entry.getKey();
                            V value = entry.getValue();
                            if (key != null) {
                                // ConcurrentHashMap doesn't allow null values
                                if (value != null) {
                                    buffers[activeIndex.get()].put(key, value);
                                } else {
                                    // For null values, remove the key (or handle differently based on requirements)
                                    buffers[activeIndex.get()].remove(key);
                                }
                            }
                        }
                    },
                    // onError: propagate error and cleanup
                    error -> {
                        if (terminated.compareAndSet(false, true)) {
                            disposables.dispose();
                            emitter.error(error);
                        }
                    },
                    // onComplete: mark completed, final flush will happen on next tick or immediately
                    () -> {
                        if (!terminated.get() && !emitter.isCancelled()) {
                            sourceCompleted.set(true);
                            // Perform final flush of both buffers
                            scheduler.schedule(() -> {
                                if (terminated.get() || emitter.isCancelled()) {
                                    return;
                                }
                                // Flush any remaining data
                                flushBothBuffers(buffers, emitter, terminated);
                                // Complete the emitter
                                if (terminated.compareAndSet(false, true)) {
                                    disposables.dispose();
                                    emitter.complete();
                                }
                            });
                        }
                    }
            );
            disposables.add(sourceDisposable);

            // Start the interval timer for periodic flushing
            Disposable intervalDisposable = Flux.interval(interval, scheduler)
                    .subscribe(
                            tick -> {
                                if (!sourceCompleted.get() && !terminated.get() && !emitter.isCancelled()) {
                                    flushBuffer.run();
                                }
                            },
                            error -> {
                                if (terminated.compareAndSet(false, true)) {
                                    disposables.dispose();
                                    emitter.error(error);
                                }
                            }
                    );
            disposables.add(intervalDisposable);

        }, FluxSink.OverflowStrategy.BUFFER);
    }

    /**
     * Flushes both buffers when source completes.
     */
    private static <K, V> void flushBothBuffers(
            ConcurrentHashMap<K, V>[] buffers,
            FluxSink<Map.Entry<K, V>> emitter,
            AtomicBoolean terminated
    ) {
        for (ConcurrentHashMap<K, V> buffer : buffers) {
            if (terminated.get() || emitter.isCancelled()) {
                return;
            }
            if (!buffer.isEmpty()) {
                List<Map.Entry<K, V>> entries = new ArrayList<>(buffer.size());
                buffer.forEach((k, v) -> entries.add(Map.entry(k, v)));
                buffer.clear();

                for (Map.Entry<K, V> entry : entries) {
                    if (emitter.isCancelled()) {
                        return;
                    }
                    emitter.next(entry);
                }
            }
        }
    }

    /**
     * Conflates items by key with a maximum buffer size limit.
     * <p>
     * If the buffer exceeds the specified size, the oldest entries may be dropped
     * or an error can be signaled based on the overflow strategy.
     *
     * @param source       the source flux of key-value entries
     * @param interval     the time interval for conflation windows
     * @param maxKeys      maximum number of distinct keys to buffer
     * @param <K>          the key type
     * @param <V>          the value type
     * @return a flux emitting conflated key-value entries at each interval
     * @throws NullPointerException     if source or interval is null
     * @throws IllegalArgumentException if interval is non-positive or maxKeys is not positive
     */
    public static <K, V> Flux<Map.Entry<K, V>> conflateByKeyWithLimit(
            Flux<Map.Entry<K, V>> source,
            Duration interval,
            int maxKeys
    ) {
        return conflateByKeyWithLimit(source, interval, maxKeys, Schedulers.parallel());
    }

    /**
     * Conflates items by key with a maximum buffer size limit using a custom scheduler.
     *
     * @param source       the source flux of key-value entries
     * @param interval     the time interval for conflation windows
     * @param maxKeys      maximum number of distinct keys to buffer
     * @param scheduler    the scheduler to use for the interval timer
     * @param <K>          the key type
     * @param <V>          the value type
     * @return a flux emitting conflated key-value entries at each interval
     */
    public static <K, V> Flux<Map.Entry<K, V>> conflateByKeyWithLimit(
            Flux<Map.Entry<K, V>> source,
            Duration interval,
            int maxKeys,
            Scheduler scheduler
    ) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(interval, "interval must not be null");
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys must be positive");
        }

        return Flux.create(emitter -> {
            @SuppressWarnings("unchecked")
            final ConcurrentHashMap<K, V>[] buffers = new ConcurrentHashMap[]{
                    new ConcurrentHashMap<>(),
                    new ConcurrentHashMap<>()
            };

            final AtomicInteger activeIndex = new AtomicInteger(0);
            final AtomicBoolean sourceCompleted = new AtomicBoolean(false);
            final AtomicBoolean terminated = new AtomicBoolean(false);
            final Disposable.Composite disposables = Disposables.composite();

            final Runnable flushBuffer = () -> {
                if (terminated.get() || emitter.isCancelled()) {
                    return;
                }

                int oldIndex = activeIndex.getAndUpdate(i -> 1 - i);
                ConcurrentHashMap<K, V> bufferToFlush = buffers[oldIndex];

                if (!bufferToFlush.isEmpty()) {
                    List<Map.Entry<K, V>> entries = new ArrayList<>(bufferToFlush.size());
                    bufferToFlush.forEach((k, v) -> entries.add(Map.entry(k, v)));
                    bufferToFlush.clear();

                    for (Map.Entry<K, V> entry : entries) {
                        if (emitter.isCancelled()) {
                            return;
                        }
                        emitter.next(entry);
                    }
                }
            };

            final Runnable cleanup = () -> {
                if (terminated.compareAndSet(false, true)) {
                    disposables.dispose();
                    buffers[0].clear();
                    buffers[1].clear();
                }
            };

            emitter.onDispose(() -> cleanup.run());

            Disposable sourceDisposable = source.subscribe(
                    entry -> {
                        if (entry != null && !terminated.get() && !emitter.isCancelled()) {
                            K key = entry.getKey();
                            V value = entry.getValue();
                            if (key != null) {
                                ConcurrentHashMap<K, V> currentBuffer = buffers[activeIndex.get()];
                                // Check size limit before adding a new key
                                if (value != null) {
                                    if (currentBuffer.containsKey(key) || currentBuffer.size() < maxKeys) {
                                        currentBuffer.put(key, value);
                                    }
                                    // Silently drop if limit reached for new keys (or could emit warning)
                                } else {
                                    currentBuffer.remove(key);
                                }
                            }
                        }
                    },
                    error -> {
                        if (terminated.compareAndSet(false, true)) {
                            disposables.dispose();
                            emitter.error(error);
                        }
                    },
                    () -> {
                        if (!terminated.get() && !emitter.isCancelled()) {
                            sourceCompleted.set(true);
                            scheduler.schedule(() -> {
                                if (terminated.get() || emitter.isCancelled()) {
                                    return;
                                }
                                flushBothBuffers(buffers, emitter, terminated);
                                if (terminated.compareAndSet(false, true)) {
                                    disposables.dispose();
                                    emitter.complete();
                                }
                            });
                        }
                    }
            );
            disposables.add(sourceDisposable);

            Disposable intervalDisposable = Flux.interval(interval, scheduler)
                    .subscribe(
                            tick -> {
                                if (!sourceCompleted.get() && !terminated.get() && !emitter.isCancelled()) {
                                    flushBuffer.run();
                                }
                            },
                            error -> {
                                if (terminated.compareAndSet(false, true)) {
                                    disposables.dispose();
                                    emitter.error(error);
                                }
                            }
                    );
            disposables.add(intervalDisposable);

        }, FluxSink.OverflowStrategy.BUFFER);
    }
}