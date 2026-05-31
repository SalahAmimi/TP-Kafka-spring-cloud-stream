package org.kafka.kafkaspringcloudstream.controllers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.kafka.kafkaspringcloudstream.events.PageEvent;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import org.springframework.http.codec.ServerSentEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@RestController
public class PageEventController {

    private StreamBridge streamBridge;
    private InteractiveQueryService interactiveQueryService;

    public PageEventController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping("/publish")
    public PageEvent publish(String name, String topic){
        PageEvent event =  new PageEvent(
                                name,
                                Math.random()>0.5 ? "U1":"U2",
                                new Date(),
                                10+new Random().nextInt(1000)
        );
        streamBridge.send(topic, event);
        return event;
    }

    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {

        return Flux.create(sink -> {

            Runnable task = () -> {

                try {
                    Map<String, Long> result = new HashMap<>();

                    ReadOnlyWindowStore<String, Long> store =
                            interactiveQueryService.getQueryableStore(
                                    "count-store",
                                    QueryableStoreTypes.windowStore()
                            );
                    if (store == null) {
                        System.out.println("STORE IS NULL ");
                        return;
                    }

                    Instant now = Instant.now();
                    Instant from = now.minusSeconds(10);
                    KeyValueIterator<Windowed<String>, Long> iter =
                            store.fetchAll(from, now);

                    while (iter.hasNext()) {
                        KeyValue<Windowed<String>, Long> next = iter.next();
                        result.put(next.key.key(), next.value);
                    }

                    iter.close();
                    //System.out.println("SSE DATA → " + result);
                    sink.next(result);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            };

            // send every 1 second
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(task, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
        });
    }
}
