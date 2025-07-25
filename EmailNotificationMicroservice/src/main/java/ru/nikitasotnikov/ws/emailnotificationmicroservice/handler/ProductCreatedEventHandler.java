package ru.nikitasotnikov.ws.emailnotificationmicroservice.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.nikitasotnikov.ws.core.ProductCreatedEvent;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {
    private static final Logger log = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){
        log.info("Received event: {}", productCreatedEvent.getTitle());
    }
}
