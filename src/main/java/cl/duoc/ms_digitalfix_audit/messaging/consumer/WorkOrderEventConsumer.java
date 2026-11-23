package cl.duoc.ms_digitalfix_audit.messaging.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import cl.duoc.ms_digitalfix_audit.messaging.WorkOrderEvent;
import cl.duoc.ms_digitalfix_audit.messaging.WorkOrderEventParser;
import cl.duoc.ms_digitalfix_audit.service.AuditService;

/**
 * Consume digitalfix.workorders.events con el grupo "audit-timeline" (fijado
 * en KafkaConsumerConfig). La concurrencia (una por particion) tambien se
 * configura alla, no aqui.
 *
 * El parseo y el guardado son los unicos dos pasos, y cada uno lanza su propia
 * excepcion de negocio si falla (ErrorNoRecuperableException o
 * ErrorRecuperableException); ninguna se atrapa aqui porque es el
 * CommonErrorHandler del contenedor (ver KafkaConsumerConfig) quien decide
 * reintentar o derivar a la DLT. Si el metodo retorna sin lanzar, el
 * contenedor confirma el offset (AckMode.RECORD es el default del container
 * factory).
 */
@Component
public class WorkOrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkOrderEventConsumer.class);

    @Autowired
    private WorkOrderEventParser parser;

    @Autowired
    private AuditService service;

    @KafkaListener(topics = "${digitalfix.kafka.topics.workorders-events}")
    public void recibir(ConsumerRecord<String, String> record) {
        WorkOrderEvent evento = parser.parsear(record.value());

        service.registrar(evento);

        log.info("[audit] evento {} de la orden {} consumido de la particion {}",
                evento.eventId(), evento.ordenId(), record.partition());
    }
}
