package com.tekion.library.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tekion.library.domain.Book;
import com.tekion.library.domain.LibraryEvent;
import com.tekion.library.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventControllerUnitTests {


    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper mapper = new ObjectMapper();

    @Test
    void postLibraryEvent4xx() throws Exception {
        Book book = Book.builder().bookId(null).bookAuthor(null).bookName("Kafka").build();

        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).libraryEventType(null).book(null)
                                                .build();
        String json = mapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(post("/v1/libraryeevents").content(json).contentType(MediaType.APPLICATION_JSON))
               .andExpect(status().is4xxClientError());
    }

    @Test
    void postLibraryEvent() throws Exception {
        Book book = Book.builder().bookId(123).bookAuthor("Ankit").bookName("Kafka").build();
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).libraryEventType(null).book(book)
                                                .build();
        String json = mapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);


        mockMvc.perform(post("/v1/libraryeevents").content(json).contentType(MediaType.APPLICATION_JSON))
               .andExpect(status().isCreated());
    }
}
