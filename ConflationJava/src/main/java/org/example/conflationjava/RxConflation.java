package org.example.conflationjava;

import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;

@Service
public class RxConflation {

    Logger logger = LoggerFactory.getLogger(RxConflation.class);


    private final Sinks.EmitFailureHandler failureHandler = (signalType, emitResult) -> {
        logger.debug("Signal Type : {} Emit Result : {}", signalType, emitResult);
        return emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED);
    };

    @PostConstruct
    protected void Init() {
        {
            logger.info("RxConflation Init");

            // Create Subject
            // Create a Flux that emits a pair of values every 100 milliseconds
            // and conflates them by key using the conflateByKey method

            Sinks.Many<Map.Entry<String,Long>> stream = Sinks.many().multicast().onBackpressureBuffer();


            // Example usage
            Flux.interval(Duration.ofMillis(100)).subscribe(aLong -> {


               Map.Entry<String,Long> entry = Pair.of("KeyTest1", aLong);
                logger.info("Emitting: Stream 1 {}", entry);

                stream.emitNext(entry, failureHandler);
            });


            Flux.interval(Duration.ofMillis(100)).subscribe(aLong -> {


                Map.Entry<String,Long> entry = Pair.of("KeyTest2", aLong);
                logger.info("Emitting: Stream 2 {}", entry);

                stream.emitNext(entry, failureHandler);
            });



            Flux<Map.Entry<String, Long>> flux = stream.asFlux();

            ReactorExtensions.conflateByKey(flux, Duration.ofSeconds(1))
                    .subscribe(entry -> {
                        logger.info("Conflated Key: {}, Value: {}", entry.getKey(), entry.getValue());
                    }, error -> {
                        logger.error("Error: {}", error.getMessage());
                    }, () -> {
                        logger.info("Completed");
                    });

        }
    }


}
