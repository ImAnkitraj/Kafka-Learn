package com.tekion.library.controller;

import com.tekion.library.domain.Book;
import com.tekion.library.domain.LibraryEvent;
import com.tekion.library.enums.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" })

class LibraryApplicationTestsIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    private Consumer<Integer, String> consumer;


    @BeforeEach
    void setUp() {

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true",
                embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),
                new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void posts() {
        Book book = Book.builder().bookId(123).bookAuthor("Ankit").bookName("Kafka").build();
        LibraryEvent libraryEvent =
                LibraryEvent.builder().libraryEventId(null).libraryEventType(null).book(book).build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);
        ResponseEntity<LibraryEvent> response = restTemplate.exchange("/v1/libraryeevents", HttpMethod.POST, request,
                LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = singleRecord.value();
        String expectedValue = "{" + "\"libraryEventId\":null," + "\"libraryEventType\":\"NEW\"," +
                               "\"book\":{" +
                               "\"bookId\":123," + "\"bookName\":\"Kafka\"," +

                               "\"bookAuthor\":\"Ankit\"" + "}" + "}";

        System.out.println("--------------------" + value);
        assertEquals(expectedValue, value);
    }

}
