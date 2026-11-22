package cl.duoc.ms_digitalfix_audit.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Se declara el ObjectMapper a mano, igual que RabbitProducerConfig en
 * workorders: en este proyecto no hay garantia de que la autoconfiguracion de
 * Jackson registre un bean autoescaneable solo por tener jackson-databind en
 * el classpath, y WorkOrderEventParser y AuditEventPublisher lo necesitan
 * inyectado.
 *
 * FAIL_ON_UNKNOWN_PROPERTIES en false porque workorders puede agregar campos
 * al evento antes de que audit los use; un campo nuevo no debe tumbar el
 * consumo.
 *
 * WRITE_DATES_AS_TIMESTAMPS en false: sin esto, Instant se serializa como
 * [segundos, nanos] en vez de texto ISO-8601 al publicar en
 * digitalfix.audit.events, que es el formato que espera el contrato del
 * evento (y el que parsea report al leer ese topico).
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
