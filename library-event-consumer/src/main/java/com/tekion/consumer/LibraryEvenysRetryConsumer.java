package com.tekion.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tekion.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEvenysRetryConsumer {

    @Autowired
    LibraryEventService service;

    @KafkaListener(topics = { "${topics.retry}" }, groupId = "retry-listener-group",
                   autoStartup = "${retryListener.startup: true}")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("consumer record retry -------------{}", consumerRecord);
        consumerRecord.headers().forEach(header -> {
            log.info("key: {}, value: {}", header.key(), header.value());
        });
        service.processLibraryEvent(consumerRecord);
    }
}
