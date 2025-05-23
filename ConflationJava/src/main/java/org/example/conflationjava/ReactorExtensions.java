package org.example.conflationjava;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ReactorExtensions {
    public static <K, V> Flux<Map.Entry<K, V>> conflateByKey(
            Flux<Map.Entry<K, V>> source,
            Duration interval
    ) {
        return Flux.create(emitter -> {
            ConcurrentHashMap<K, V> buffer1 = new ConcurrentHashMap<>();
            ConcurrentHashMap<K, V> buffer2 = new ConcurrentHashMap<>();
            AtomicReference<ConcurrentHashMap<K, V>> currentBuffer = new AtomicReference<>(buffer1);

            source.subscribe(pair -> currentBuffer.get().put(pair.getKey(), pair.getValue()),
                    emitter::error,
                    emitter::complete);

            Flux.interval(interval, Schedulers.parallel())
                    .subscribe(tick -> {
                        ConcurrentHashMap<K, V> oldBuffer = currentBuffer.getAndSet(
                                currentBuffer.get() == buffer1 ? buffer2 : buffer1
                        );
                        oldBuffer.forEach((k, v) -> emitter.next(Map.entry(k, v)));
                        oldBuffer.clear();
                    }, emitter::error);
        }, FluxSink.OverflowStrategy.BUFFER);
    }
}