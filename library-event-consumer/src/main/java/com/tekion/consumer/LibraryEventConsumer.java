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
public class LibraryEventConsumer {

    @Autowired
    LibraryEventService service;
    @KafkaListener(topics = {"library-events"}, groupId = "library-events-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("consumer record -------------{}",consumerRecord);
        service.processLibraryEvent(consumerRecord);
    }
}
