package cl.duoc.ms_digitalfix_audit.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import cl.duoc.ms_digitalfix_audit.exception.ErrorNoRecuperableException;

/**
 * Convierte el valor crudo del ConsumerRecord (texto JSON) en WorkOrderEvent y
 * valida que traiga los campos obligatorios.
 *
 * Se hace fuera del deserializador de Kafka a proposito: un deserializador que
 * lanza una excepcion dentro del cliente deja el consumidor atascado en ese
 * mensaje sin poder avanzar. Aqui el valor llega siempre como String y el
 * fallo se maneja como una excepcion de negocio mas, que el error handler de
 * KafkaConsumerConfig ya sabe derivar a la DLT.
 */
@Component
public class WorkOrderEventParser {

    private final ObjectMapper mapper;

    public WorkOrderEventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public WorkOrderEvent parsear(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new ErrorNoRecuperableException("El mensaje llego vacio");
        }

        WorkOrderEvent evento;
        try {
            evento = mapper.readValue(payload, WorkOrderEvent.class);
        } catch (JsonProcessingException e) {
            throw new ErrorNoRecuperableException(
                    "El mensaje no es un JSON valido: " + e.getOriginalMessage(), e);
        }

        if (evento == null || !evento.esValido()) {
            throw new ErrorNoRecuperableException(
                    "El evento no trae los campos obligatorios "
                            + "(eventId, eventType, ordenId, estadoNuevo, occurredAt): " + payload);
        }

        return evento;
    }
}
