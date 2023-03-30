package com.tekion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tekion.entity.LibraryEvent;
import com.tekion.jpa.LibrarEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    private LibrarEventRepository repository;
    @Autowired
    ObjectMapper mapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("service ----> libraryEVent : {}", libraryEvent);

        if (libraryEvent != null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("default");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("LibraryEvent id is missing");
        }
        Optional<LibraryEvent> optional = repository.findById(libraryEvent.getLibraryEventId());
        if (optional.isPresent()) {
            throw new IllegalArgumentException("Not a valid library event");
        }

        log.info("Validation success for library event {}", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        log.info("Successfully persisted {}", libraryEvent);
    }
}
