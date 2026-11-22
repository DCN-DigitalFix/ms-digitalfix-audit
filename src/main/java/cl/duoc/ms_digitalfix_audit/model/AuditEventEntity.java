package cl.duoc.ms_digitalfix_audit.model;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * La clave primaria es el eventId del mensaje de Kafka, no un id autogenerado.
 * Es lo que hace idempotente a AuditService.registrar: Kafka entrega al menos
 * una vez, y guardar dos veces el mismo eventId choca contra esta clave en vez
 * de duplicar la fila.
 */
@Entity
@Table(name = "audit_events")
public class AuditEventEntity {

    @Id
    @Column(name = "event_id", nullable = false, length = 64)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 40)
    private String eventType;

    @Column(name = "orden_id", nullable = false, length = 20)
    private String ordenId;

    @Column(name = "estado_anterior", length = 20)
    private String estadoAnterior;

    @Column(name = "estado_nuevo", nullable = false, length = 20)
    private String estadoNuevo;

    @Column(name = "cliente", length = 120)
    private String cliente;

    @Column(name = "tecnico_asignado", length = 120)
    private String tecnicoAsignado;

    @Column(name = "actor", length = 120)
    private String actor;

    @Column(name = "trace_id", length = 64)
    private String traceId;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "registrado_en", nullable = false)
    private Instant registradoEn;

    public AuditEventEntity() {
    }

    public AuditEventEntity(String eventId, String eventType, String ordenId, String estadoAnterior,
                             String estadoNuevo, String cliente, String tecnicoAsignado, String actor,
                             String traceId, Instant occurredAt, Instant registradoEn) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.ordenId = ordenId;
        this.estadoAnterior = estadoAnterior;
        this.estadoNuevo = estadoNuevo;
        this.cliente = cliente;
        this.tecnicoAsignado = tecnicoAsignado;
        this.actor = actor;
        this.traceId = traceId;
        this.occurredAt = occurredAt;
        this.registradoEn = registradoEn;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getOrdenId() { return ordenId; }
    public void setOrdenId(String ordenId) { this.ordenId = ordenId; }
    public String getEstadoAnterior() { return estadoAnterior; }
    public void setEstadoAnterior(String estadoAnterior) { this.estadoAnterior = estadoAnterior; }
    public String getEstadoNuevo() { return estadoNuevo; }
    public void setEstadoNuevo(String estadoNuevo) { this.estadoNuevo = estadoNuevo; }
    public String getCliente() { return cliente; }
    public void setCliente(String cliente) { this.cliente = cliente; }
    public String getTecnicoAsignado() { return tecnicoAsignado; }
    public void setTecnicoAsignado(String tecnicoAsignado) { this.tecnicoAsignado = tecnicoAsignado; }
    public String getActor() { return actor; }
    public void setActor(String actor) { this.actor = actor; }
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }
    public Instant getRegistradoEn() { return registradoEn; }
    public void setRegistradoEn(Instant registradoEn) { this.registradoEn = registradoEn; }
}
