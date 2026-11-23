package cl.duoc.ms_digitalfix_audit.messaging.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import cl.duoc.ms_digitalfix_audit.config.DigitalfixKafkaProperties;
import cl.duoc.ms_digitalfix_audit.model.AuditEventEntity;
import cl.duoc.ms_digitalfix_audit.messaging.AuditRecordEvent;

/**
 * Unico punto del microservicio que conoce KafkaTemplate para publicar.
 * AuditService le informa que un evento ya se guardo y no sabe a que topico
 * va ni con que clave.
 *
 * Igual que WorkOrderEventPublisher en workorders: la mensajeria es
 * secundaria respecto de la auditoria misma. Si no se puede publicar en
 * digitalfix.audit.events, el evento ya quedo guardado y el consumo del
 * topico de origen no se interrumpe; el fallo solo se registra en el log.
 */
@Service
public class AuditEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(AuditEventPublisher.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private DigitalfixKafkaProperties props;

    public void publicarRegistroDeAuditoria(AuditEventEntity entidad) {
        AuditRecordEvent evento = AuditRecordEvent.de(entidad);
        String topicoAuditEvents = props.topics().auditEvents();

        String payload;
        try {
            payload = mapper.writeValueAsString(evento);
        } catch (JsonProcessingException e) {
            log.error("[audit] no se pudo serializar el registro de auditoria de la orden {}: {}",
                    entidad.getOrdenId(), e.getMessage());
            return;
        }

        // Misma clave que el topico de origen: todos los registros de una
        // orden caen en la misma particion y se leen en el orden en que
        // audit los proceso.
        // send() puede fallar de forma sincrona (metadatos, id de productor
        // idempotente) fuera del alcance de whenComplete.
        try {
            kafkaTemplate.send(topicoAuditEvents, entidad.getOrdenId(), payload)
                    .whenComplete((resultado, error) -> {
                        if (error != null) {
                            log.error("[audit] no se pudo publicar el registro {} en {}: {}",
                                    evento.eventId(), topicoAuditEvents, error.getMessage());
                        } else {
                            log.info("[audit] registro {} publicado en {} particion {}",
                                    evento.eventId(), topicoAuditEvents,
                                    resultado.getRecordMetadata().partition());
                        }
                    });
        } catch (RuntimeException e) {
            log.error("[audit] no se pudo publicar el registro {} en {}: {}",
                    evento.eventId(), topicoAuditEvents, e.getMessage());
        }
    }
}
