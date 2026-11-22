package cl.duoc.ms_digitalfix_audit.service;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import cl.duoc.ms_digitalfix_audit.dto.AuditEventView;
import cl.duoc.ms_digitalfix_audit.entity.AuditEventEntity;
import cl.duoc.ms_digitalfix_audit.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_audit.exception.NotFoundException;
import cl.duoc.ms_digitalfix_audit.messaging.WorkOrderEvent;
import cl.duoc.ms_digitalfix_audit.messaging.producer.AuditEventPublisher;
import cl.duoc.ms_digitalfix_audit.repository.AuditEventRepository;

/**
 * Logica de negocio de la auditoria, sin conocer Kafka: WorkOrderEventConsumer
 * le entrega un WorkOrderEvent ya parseado y validado, y AuditController le
 * pide vistas de lectura. Ninguno de los dos conoce como se guarda.
 */
@Service
public class AuditService {

    private static final Logger log = LoggerFactory.getLogger(AuditService.class);

    private static final int LIMITE_MINIMO = 1;
    private static final int LIMITE_MAXIMO = 500;

    private final AuditEventRepository repository;
    private final AuditEventPublisher publisher;

    public AuditService(AuditEventRepository repository, AuditEventPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    /**
     * Idempotente: Kafka entrega al menos una vez, asi que el mismo eventId
     * puede llegar dos veces. La verificacion previa cubre el caso normal (el
     * commit del offset anterior se perdio) y el saveAndFlush con captura de
     * DataIntegrityViolationException cubre la carrera residual.
     */
    public void registrar(WorkOrderEvent evento) {
        if (repository.existsById(evento.eventId())) {
            log.info("[audit] evento {} ya registrado, se descarta el duplicado", evento.eventId());
            return;
        }

        AuditEventEntity entidad = new AuditEventEntity(
                evento.eventId(),
                evento.eventType(),
                evento.ordenId(),
                evento.estadoAnterior(),
                evento.estadoNuevo(),
                evento.cliente(),
                evento.tecnicoAsignado(),
                evento.actor(),
                evento.traceId(),
                evento.occurredAt(),
                Instant.now());

        try {
            repository.saveAndFlush(entidad);
        } catch (DataIntegrityViolationException e) {
            log.info("[audit] evento {} llego duplicado en una carrera, se descarta", evento.eventId());
            return;
        } catch (DataAccessException e) {
            // Transitorio (la base no respondio): el error handler de Kafka
            // reintenta con backoff antes de derivar a la DLT.
            throw new ErrorRecuperableException(
                    "No se pudo guardar el evento " + evento.eventId() + " en la base de datos", e);
        }

        log.info("[audit] evento {} de la orden {} guardado ({} -> {})",
                evento.eventId(), evento.ordenId(), evento.estadoAnterior(), evento.estadoNuevo());

        publisher.publicarRegistroDeAuditoria(entidad);
    }

    public List<AuditEventView> listarUltimos(int limite) {
        int tam = Math.max(LIMITE_MINIMO, Math.min(limite, LIMITE_MAXIMO));
        return repository.findAllByOrderByOccurredAtDesc(PageRequest.of(0, tam))
                .stream()
                .map(AuditEventView::de)
                .toList();
    }

    public List<AuditEventView> timelineDeOrden(String ordenId) {
        List<AuditEventEntity> eventos = repository.findByOrdenIdOrderByOccurredAtAsc(ordenId);
        if (eventos.isEmpty()) {
            throw new NotFoundException("No hay eventos de auditoria para la orden " + ordenId);
        }
        return eventos.stream().map(AuditEventView::de).toList();
    }
}
