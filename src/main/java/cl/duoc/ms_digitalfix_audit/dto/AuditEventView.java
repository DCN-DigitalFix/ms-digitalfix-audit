package cl.duoc.ms_digitalfix_audit.dto;

import java.time.Instant;

import cl.duoc.ms_digitalfix_audit.entity.AuditEventEntity;

/** Forma expuesta por la API. Separada de la entidad para no filtrar detalles de persistencia. */
public record AuditEventView(
        String eventId,
        String eventType,
        String ordenId,
        String estadoAnterior,
        String estadoNuevo,
        String cliente,
        String tecnicoAsignado,
        String actor,
        Instant occurredAt,
        Instant registradoEn) {

    public static AuditEventView de(AuditEventEntity entidad) {
        return new AuditEventView(
                entidad.getEventId(),
                entidad.getEventType(),
                entidad.getOrdenId(),
                entidad.getEstadoAnterior(),
                entidad.getEstadoNuevo(),
                entidad.getCliente(),
                entidad.getTecnicoAsignado(),
                entidad.getActor(),
                entidad.getOccurredAt(),
                entidad.getRegistradoEn());
    }
}
