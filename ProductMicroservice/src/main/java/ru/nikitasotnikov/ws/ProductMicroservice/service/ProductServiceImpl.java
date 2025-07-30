package ru.nikitasotnikov.ws.ProductMicroservice.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.nikitasotnikov.ws.ProductMicroservice.service.dto.CreateProductDto;
import ru.nikitasotnikov.ws.core.ProductCreatedEvent;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class ProductServiceImpl implements ProductService{
    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException {
        //TODO save to DB
        String productId = UUID.randomUUID().toString();
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
                productId,
                createProductDto.getTitle(),
                createProductDto.getPrice(),
                createProductDto.getQuantity());

        ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(
                "product-created-events-topic",
                productId,
                productCreatedEvent
        );

        record.headers().add("messageId", "qwerty".getBytes());

        SendResult<String, ProductCreatedEvent> result = kafkaTemplate.send(record).get();

        log.info("Topic: {}", result.getRecordMetadata().topic());
        log.info("Partition: {}", result.getRecordMetadata().partition());
        log.info("Offset: {}", result.getRecordMetadata().offset());

        log.info("Return {}", productId);

        return productId;
    }
}
