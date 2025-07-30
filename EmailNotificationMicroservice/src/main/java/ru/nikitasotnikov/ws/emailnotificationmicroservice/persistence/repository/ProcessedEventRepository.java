package ru.nikitasotnikov.ws.emailnotificationmicroservice.persistence.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.nikitasotnikov.ws.emailnotificationmicroservice.persistence.entity.ProcessedEventEntity;

import java.util.Optional;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {
    Optional<ProcessedEventEntity> findByMessageId(String messageId);
}
