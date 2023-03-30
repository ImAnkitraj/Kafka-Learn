package com.tekion.library.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tekion.library.domain.Book;
import com.tekion.library.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();
    @InjectMocks
    LibraryEventProducer producer;
    @Test
    void sendLibraryEventApproach2OnFailure() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder().bookId(123).bookAuthor("Ankit").bookName("Kafka").build();

        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).libraryEventType(null).book(null)
                                                .build();

        SettableListenableFuture future = new SettableListenableFuture();

        future.setException(new RuntimeException("Exception calling kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        assertThrows(Exception.class,()->producer.sendLibraryEventApproach2(libraryEvent).get());
    }

    @Test
    void sendLibraryEventApproach2OnSuccess() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder().bookId(123).bookAuthor("Ankit").bookName("Kafka").build();

        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).libraryEventType(null).book(null)
                                                .build();

        SettableListenableFuture future = new SettableListenableFuture();

        String record = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events",
                libraryEvent.getLibraryEventId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),1,1,342,
                System.currentTimeMillis(),1,2);
        SendResult<Integer, String> result = new SendResult<>(producerRecord, recordMetadata);
        future.set(result);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        ListenableFuture<SendResult<Integer, String >> result1 = producer.sendLibraryEventApproach2(libraryEvent);
        SendResult<Integer, String> result2 = result1.get();
        assert result2.getRecordMetadata().partition() == 1;
    }
}
