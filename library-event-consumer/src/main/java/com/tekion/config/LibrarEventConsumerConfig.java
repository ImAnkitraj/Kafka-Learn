package com.tekion.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibrarEventConsumerConfig {


    @Autowired
    KafkaTemplate templtae;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;
    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templtae, (r, e) -> {
            if(e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            }
            return new TopicPartition(dltTopic, r.partition());

        });
        return recoverer;
    }
    public DefaultErrorHandler errorHandler() {

        var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
        var exceptionsToRetryList = List.of(RecoverableDataAccessException.class);
        FixedBackOff fixedBackOff = new FixedBackOff(1000l, 2);
        ExponentialBackOffWithMaxRetries exponentialBackOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOffWithMaxRetries.setInitialInterval(100l);
        exponentialBackOffWithMaxRetries.setMultiplier(2.0);
        exponentialBackOffWithMaxRetries.setMaxInterval(2_000l);
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(
                publishingRecoverer(),
                //                fixedBackOff
                exponentialBackOffWithMaxRetries);

        exceptionsToIgnoreList.forEach(defaultErrorHandler::addNotRetryableExceptions);
        exceptionsToRetryList.forEach(defaultErrorHandler::addNotRetryableExceptions);
        //used to debug for each and every attempt
        defaultErrorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed recoed in rery listener, exception: {}, deliveryAttemp: {}", ex, deliveryAttempt);
        }));
        return defaultErrorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);

        factory.setCommonErrorHandler(errorHandler());
        //        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
