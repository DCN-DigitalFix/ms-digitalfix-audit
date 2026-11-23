package cl.duoc.ms_digitalfix_audit.messaging.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;

import cl.duoc.ms_digitalfix_audit.messaging.WorkOrderEvent;
import cl.duoc.ms_digitalfix_audit.messaging.WorkOrderEventParser;
import cl.duoc.ms_digitalfix_audit.service.AuditService;

/**
 * El consumidor no decide nada por si mismo: delega el parseo y el guardado,
 * y propaga cualquier excepcion que ellos lancen sin atraparla (ver
 * KafkaConsumerConfig, que es quien interpreta esas excepciones).
 */
@ExtendWith(MockitoExtension.class)
class WorkOrderEventConsumerTest {

    @Mock
    private WorkOrderEventParser parser;

    @Mock
    private AuditService service;

    /** WorkOrderEventConsumer se inyecta por campo: no tiene constructor que invocar. */
    private WorkOrderEventConsumer crearConsumer() {
        WorkOrderEventConsumer consumer = new WorkOrderEventConsumer();
        ReflectionTestUtils.setField(consumer, "parser", parser);
        ReflectionTestUtils.setField(consumer, "service", service);
        return consumer;
    }

    @Test
    void delegaElParseoYElGuardadoEnOrden() {
        WorkOrderEventConsumer consumer = crearConsumer();

        WorkOrderEvent evento = new WorkOrderEvent(
                "e-1", "ORDEN_CREADA", Instant.now(), "trace-1",
                "OT-0001", null, "INGRESADA", "Cliente Prueba", null, "sistema");

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("digitalfix.workorders.events", 0, 10L, "OT-0001", "{...}");

        when(parser.parsear("{...}")).thenReturn(evento);

        consumer.recibir(record);

        verify(parser).parsear(eq("{...}"));
        verify(service).registrar(any(WorkOrderEvent.class));
    }

    @Test
    void unPayloadInvalidoSePropagaSinLlamarAlServicio() {
        WorkOrderEventConsumer consumer = crearConsumer();

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("digitalfix.workorders.events", 0, 11L, "OT-0002", "{no es json");

        when(parser.parsear("{no es json"))
                .thenThrow(new cl.duoc.ms_digitalfix_audit.exception.ErrorNoRecuperableException("invalido"));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> consumer.recibir(record))
                .isInstanceOf(cl.duoc.ms_digitalfix_audit.exception.ErrorNoRecuperableException.class);

        verify(service, org.mockito.Mockito.never()).registrar(any());
    }
}
