package cl.duoc.ms_digitalfix_audit.messaging;

import java.time.Instant;
import java.util.UUID;

import cl.duoc.ms_digitalfix_audit.entity.AuditEventEntity;

/**
 * Evento que audit publica en digitalfix.audit.events despues de guardar un
 * WorkOrderEvent. Es un registro normalizado, pensado para que otros
 * microservicios (notify, report) lo consuman sin conocer el formato interno
 * de workorders ni acoplarse a su topico.
 *
 * @param eventId       identificador propio de este evento, distinto del origen
 * @param origenEventId eventId del WorkOrderEvent que genero este registro, para trazar
 */
public record AuditRecordEvent(
        String eventId,
        String origenEventId,
        String ordenId,
        String eventType,
        String estadoAnterior,
        String estadoNuevo,
        Instant occurredAt,
        Instant registradoEn,
        String traceId) {

    public static AuditRecordEvent de(AuditEventEntity entidad) {
        return new AuditRecordEvent(
                UUID.randomUUID().toString(),
                entidad.getEventId(),
                entidad.getOrdenId(),
                entidad.getEventType(),
                entidad.getEstadoAnterior(),
                entidad.getEstadoNuevo(),
                entidad.getOccurredAt(),
                entidad.getRegistradoEn(),
                entidad.getTraceId());
    }
}
