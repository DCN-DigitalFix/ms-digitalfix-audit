package cl.duoc.ms_digitalfix_audit.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.Pageable;

import cl.duoc.ms_digitalfix_audit.dto.AuditEventView;
import cl.duoc.ms_digitalfix_audit.entity.AuditEventEntity;
import cl.duoc.ms_digitalfix_audit.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_audit.exception.NotFoundException;
import cl.duoc.ms_digitalfix_audit.messaging.WorkOrderEvent;
import cl.duoc.ms_digitalfix_audit.messaging.producer.AuditEventPublisher;
import cl.duoc.ms_digitalfix_audit.repository.AuditEventRepository;

@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

    @Mock
    private AuditEventRepository repository;

    @Mock
    private AuditEventPublisher publisher;

    private AuditService service;

    private final WorkOrderEvent evento = new WorkOrderEvent(
            "e-1", "ORDEN_CREADA", Instant.parse("2026-11-21T22:00:00Z"),
            "trace-1", "OT-0001", null, "INGRESADA",
            "Cliente Prueba", null, "sistema");

    @org.junit.jupiter.api.BeforeEach
    void crearServicio() {
        service = new AuditService(repository, publisher);
    }

    @Test
    void guardaYPublicaUnEventoNuevo() {
        when(repository.existsById("e-1")).thenReturn(false);

        service.registrar(evento);

        verify(repository).saveAndFlush(any(AuditEventEntity.class));
        verify(publisher).publicarRegistroDeAuditoria(any(AuditEventEntity.class));
    }

    @Test
    void descartaUnDuplicadoConocido() {
        when(repository.existsById("e-1")).thenReturn(true);

        service.registrar(evento);

        verify(repository, never()).saveAndFlush(any());
        verify(publisher, never()).publicarRegistroDeAuditoria(any());
    }

    @Test
    void descartaUnDuplicadoDeCarreraSinPropagarElError() {
        when(repository.existsById("e-1")).thenReturn(false);
        when(repository.saveAndFlush(any())).thenThrow(new DataIntegrityViolationException("duplicado"));

        service.registrar(evento);

        verify(publisher, never()).publicarRegistroDeAuditoria(any());
    }

    @Test
    void unFalloTransitorioDeLaBaseSeConvierteEnErrorRecuperable() {
        when(repository.existsById("e-1")).thenReturn(false);
        when(repository.saveAndFlush(any()))
                .thenThrow(new DataAccessResourceFailureException("sin conexion"));

        assertThatThrownBy(() -> service.registrar(evento))
                .isInstanceOf(ErrorRecuperableException.class);
    }

    @Test
    void timelineVacioLanzaNotFound() {
        when(repository.findByOrdenIdOrderByOccurredAtAsc("OT-9999")).thenReturn(List.of());

        assertThatThrownBy(() -> service.timelineDeOrden("OT-9999"))
                .isInstanceOf(NotFoundException.class);
    }

    @Test
    void timelineDevuelveLosEventosDeLaOrden() {
        AuditEventEntity entidad = new AuditEventEntity(
                "e-1", "ORDEN_CREADA", "OT-0001", null, "INGRESADA",
                "Cliente Prueba", null, "sistema", "trace-1",
                Instant.parse("2026-11-21T22:00:00Z"), Instant.now());
        when(repository.findByOrdenIdOrderByOccurredAtAsc("OT-0001")).thenReturn(List.of(entidad));

        List<AuditEventView> timeline = service.timelineDeOrden("OT-0001");

        assertThat(timeline).hasSize(1);
        assertThat(timeline.get(0).eventId()).isEqualTo("e-1");
    }

    @Test
    void listarUltimosAcotaElLimiteAlMaximoPermitido() {
        when(repository.findAllByOrderByOccurredAtDesc(any(Pageable.class))).thenReturn(List.of());

        service.listarUltimos(10_000);

        verify(repository).findAllByOrderByOccurredAtDesc(
                org.springframework.data.domain.PageRequest.of(0, 500));
    }
}
