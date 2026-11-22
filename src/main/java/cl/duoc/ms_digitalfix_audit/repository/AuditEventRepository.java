package cl.duoc.ms_digitalfix_audit.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import cl.duoc.ms_digitalfix_audit.entity.AuditEventEntity;

public interface AuditEventRepository extends JpaRepository<AuditEventEntity, String> {

    List<AuditEventEntity> findByOrdenIdOrderByOccurredAtAsc(String ordenId);

    List<AuditEventEntity> findAllByOrderByOccurredAtDesc(Pageable pageable);
}
