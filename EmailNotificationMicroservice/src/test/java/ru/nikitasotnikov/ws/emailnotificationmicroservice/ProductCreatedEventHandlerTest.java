package ru.nikitasotnikov.ws.emailnotificationmicroservice;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;
import ru.nikitasotnikov.ws.core.ProductCreatedEvent;
import ru.nikitasotnikov.ws.emailnotificationmicroservice.handler.ProductCreatedEventHandler;
import ru.nikitasotnikov.ws.emailnotificationmicroservice.persistence.entity.ProcessedEventEntity;
import ru.nikitasotnikov.ws.emailnotificationmicroservice.persistence.repository.ProcessedEventRepository;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ActiveProfiles("test")
@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerTest {

    @MockitoBean
    ProcessedEventRepository processedEventRepository;

    @MockitoBean
    RestTemplate restTemplate;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    public void testProductCreatedEventHandler_OnProductCreated_HandlesEvent() throws ExecutionException, InterruptedException {
        ProductCreatedEvent event = new ProductCreatedEvent();
        event.setPrice(new BigDecimal(100));
        event.setProductId(UUID.randomUUID().toString());
        event.setQuantity(1);
        event.setTitle("Test product");

        String messageId = UUID.randomUUID().toString();
        String messageKey = event.getProductId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "product-created-events-topic",
                messageKey,
                event
        );

        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

//        ProcessedEventEntity processedEvent = new ProcessedEventEntity();
        doReturn(Optional.empty()).when(processedEventRepository).findByMessageId(anyString());
        doReturn(null).when(processedEventRepository).save(any(ProcessedEventEntity.class));

        String responseBody = "{\"key\": \"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

        doReturn(responseEntity).when(restTemplate).exchange(anyString(), any(HttpMethod.class), isNull(), eq(String.class));

        kafkaTemplate.send(record).get();

        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(), messageIdCaptor.capture(), messageKeyCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(event.getProductId(), eventCaptor.getValue().getProductId());
    }
}
