package cl.duoc.ms_digitalfix_audit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import cl.duoc.ms_digitalfix_audit.model.AuditEventEntity;
import cl.duoc.ms_digitalfix_audit.repository.AuditEventRepository;

/**
 * Integracion real del flujo workorders -> audit, con un broker Kafka
 * embebido en vez de contra el cluster de Docker: se publica un mensaje con
 * la misma forma exacta que publica ms-digitalfix-workorders y se verifica
 * que el @KafkaListener real de audit (parser, idempotencia, persistencia en
 * H2) lo procese de punta a punta, sin mockear nada de la cadena de consumo.
 *
 * No se simula el productor de workorders en si (vive en otro modulo Maven):
 * se publica el JSON crudo con la misma forma del contrato, que es lo mismo
 * que vería el broker real sin importar quien lo haya enviado.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "digitalfix.workorders.events",
                "digitalfix.workorders.events.DLT",
                "digitalfix.audit.events"
        },
        brokerProperties = {"auto.create.topics.enable=false"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class WorkOrderToAuditIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AuditEventRepository repository;

    @Test
    void unEventoPublicadoEnWorkordersEventsQuedaGuardadoEnAudit() throws Exception {
        String payload = """
                {
                  "eventId": "it-1",
                  "eventType": "ORDEN_CREADA",
                  "occurredAt": "2026-11-22T10:00:00Z",
                  "traceId": "trace-it-1",
                  "ordenId": "OT-IT-0001",
                  "estadoNuevo": "INGRESADA",
                  "cliente": "Cliente Integracion"
                }
                """;

        kafkaTemplate.send("digitalfix.workorders.events", "OT-IT-0001", payload)
                .get(10, TimeUnit.SECONDS);

        esperarHasta(() -> repository.existsById("it-1"), 10_000);

        AuditEventEntity guardado = repository.findById("it-1").orElseThrow();
        assertThat(guardado.getOrdenId()).isEqualTo("OT-IT-0001");
        assertThat(guardado.getEstadoAnterior()).isNull();
        assertThat(guardado.getEstadoNuevo()).isEqualTo("INGRESADA");
        assertThat(guardado.getCliente()).isEqualTo("Cliente Integracion");
    }

    @Test
    void unMensajeInvalidoNoQuedaGuardadoYNoTumbaElConsumidor() throws Exception {
        kafkaTemplate.send("digitalfix.workorders.events", "OT-IT-0002", "{no es json")
                .get(10, TimeUnit.SECONDS);

        // Sin eventId no hay forma de verificar por clave primaria; se
        // confirma indirectamente: el consumidor sigue vivo y procesa el
        // siguiente mensaje valido sin quedar atascado en el invalido.
        kafkaTemplate.send("digitalfix.workorders.events", "OT-IT-0003", """
                {"eventId":"it-3","eventType":"ORDEN_CREADA","occurredAt":"2026-11-22T10:05:00Z",
                 "ordenId":"OT-IT-0003","estadoNuevo":"INGRESADA","cliente":"Cliente Tres"}
                """).get(10, TimeUnit.SECONDS);

        esperarHasta(() -> repository.existsById("it-3"), 10_000);

        assertThat(repository.findById("it-3")).isPresent();
    }

    private void esperarHasta(BooleanSupplier condicion, long timeoutMs) throws InterruptedException {
        long limite = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < limite) {
            if (condicion.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("La condicion no se cumplio dentro de " + timeoutMs + " ms");
    }
}
