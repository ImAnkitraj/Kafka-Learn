package com.tekion.library.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.tekion.library.domain.LibraryEvent;
import com.tekion.library.enums.LibraryEventType;
import com.tekion.library.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.concurrent.ExecutionException;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer producer;

    @PostMapping("/v1/libraryeevents")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("before ------------------");
        //Asynchronous
        //                producer.sendLibraryEvent(libraryEvent);
        //Synchronous
        //        SendResult<Integer, String> res = producer.sendLibraryEventSync(libraryEvent);
        //        log.info("sendResult---------- : {}", res);
        //Approach 2
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        producer.sendLibraryEventApproach2(libraryEvent);
        log.info("after ------------------");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryeevents")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException {

        if (libraryEvent.getLibraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("PLease pass the library event id");

        }
        log.info("before ------------------");
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);

        producer.sendLibraryEventApproach2(libraryEvent);
        log.info("after ------------------");

        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
