# ms-digitalfix-audit

Consumidor de Kafka del sistema DigitalFix. Escucha los eventos de dominio que
publica `ms-digitalfix-workorders`, arma la línea de tiempo de cada orden de
trabajo y publica un registro normalizado para quien quiera consumirlo sin
acoplarse al formato interno de workorders.

No expone API de escritura. Su entrada es el tópico.

---

## Topología

| Rol | Tópico | Partición/réplicas | Retención |
|---|---|---|---|
| Consume | `digitalfix.workorders.events` | 3 / 3 | 7 días |
| Produce | `digitalfix.audit.events` | 3 / 3 | 30 días |
| DLT | `digitalfix.workorders.events.DLT` | 3 / 3 | 30 días |

`min.insync.replicas=2` en los tres: con 3 réplicas, la caída de un broker no
pierde escrituras confirmadas con `acks=all`.

Grupo de consumidor: `audit-timeline`, con concurrencia 3 (una por partición).
La clave del mensaje es `ordenId`: todos los eventos de una orden caen en la
misma partición y se leen en el orden en que ocurrieron.

Ningún nombre de tópico ni parámetro de reintento está escrito a mano en una
clase: todo vive en `application.yaml` bajo `digitalfix.kafka` y se inyecta
vía `DigitalfixKafkaProperties`.

---

## Cómo trata cada mensaje

| Situación | Resultado |
|---|---|
| JSON válido, `eventId` nuevo | Se guarda, se publica en `digitalfix.audit.events`, se confirma el offset |
| `eventId` ya visto | Se descarta sin volver a guardar ni publicar; se confirma el offset |
| JSON mal formado o sin campos obligatorios | `ErrorNoRecuperableException` → directo a la DLT, sin reintentos |
| Falla la base de datos (transitorio) | `ErrorRecuperableException` → backoff exponencial (1 s, x2, tope 8 s, 3 intentos) → DLT si se agotan |

Mismo vocabulario que `ms-digitalfix-notify` usa para RabbitMQ: la diferencia
entre las dos excepciones es si el tiempo puede arreglar el problema. Un envío
mal formado no (permanente); una base de datos caída sí (recuperable).

### Idempotencia

Kafka entrega **al menos una vez**: un mismo evento puede reprocesarse si el
consumidor cae después de guardar pero antes de confirmar el offset. La clave
primaria de `audit_events` es el `eventId`, así que guardarlo dos veces choca
contra la clave en vez de duplicar la fila. `AuditService.registrar` primero
consulta `existsById` (cubre el caso normal) y además atrapa
`DataIntegrityViolationException` al guardar (cubre la carrera residual).

---

## Estructura

```
config/
  DigitalfixKafkaProperties.java   tópicos y ajustes de reintento, desde application.yaml
  KafkaConsumerConfig.java         consumer factory, error handler (backoff + DLT)
  KafkaProducerConfig.java         producer factory y KafkaTemplate
  JacksonConfig.java                ObjectMapper explícito (JavaTimeModule, tolera campos nuevos)
  SecurityConfig.java              JWT, audiencia, CORS
messaging/
  WorkOrderEvent.java              evento consumido (contrato con workorders)
  AuditRecordEvent.java           evento producido en digitalfix.audit.events
  WorkOrderEventParser.java        valida y convierte el payload crudo
  consumer/WorkOrderEventConsumer.java
  producer/AuditEventPublisher.java
entity/AuditEventEntity.java       eventId como clave primaria
repository/AuditEventRepository.java
service/AuditService.java          idempotencia, timeline, publicación del registro
dto/AuditEventView.java            forma expuesta por la API
controller/
  AuditController.java            /api/audit/events, /api/audit/orders/{id}/timeline
  HealthController.java           /api/health (público)
exception/
  ErrorRecuperableException.java
  ErrorNoRecuperableException.java
```

`AuditService` no importa nada de Kafka, y las clases de `config/` no
contienen reglas de negocio.

---

## Formato del evento consumido

```json
{
  "eventId": "e-1",
  "eventType": "ORDEN_CREADA",
  "occurredAt": "2026-11-21T22:00:00Z",
  "traceId": "req-4471",
  "ordenId": "OT-0001",
  "estadoAnterior": null,
  "estadoNuevo": "INGRESADA",
  "cliente": "Cliente Prueba",
  "tecnicoAsignado": null,
  "actor": "sistema"
}
```

Obligatorios: `eventId`, `eventType`, `ordenId`, `estadoNuevo`, `occurredAt`.
El resto es informativo y puede faltar sin invalidar el mensaje. Un campo
desconocido no lo rechaza (`FAIL_ON_UNKNOWN_PROPERTIES=false`): workorders
puede agregar campos nuevos antes de que audit los use.

---

## API

Ambos endpoints piden rol `Admin` o `Auditor` y scope `OT.Read`.

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/audit/events?limite=50` | Últimos eventos registrados, más reciente primero |
| GET | `/api/audit/orders/{ordenId}/timeline` | Eventos de una orden en orden cronológico; 404 si no hay ninguno |
| GET | `/api/health` | Público |

---

## Probarlo de punta a punta

Requiere el clúster de Kafka y Kafka UI levantados (ver el repo `infra`):

```
docker compose -f docker-compose.kafka.yml --env-file .env up -d
docker compose -f docker-compose.kafka-ui.yml --env-file .env up -d
```

```
./mvnw spring-boot:run
```

Queda en el puerto 8083. En Kafka UI (`http://localhost:8089`), producir en
`digitalfix.workorders.events` con clave `OT-0001`:

```json
{"eventId":"e-1","eventType":"ORDEN_CREADA","occurredAt":"2026-11-21T22:00:00Z","ordenId":"OT-0001","estadoNuevo":"INGRESADA","cliente":"Cliente Prueba"}
```

- El log muestra `[audit] evento e-1 de la orden OT-0001 guardado` y luego
  `registro ... publicado en digitalfix.audit.events`.
- `GET /api/audit/orders/OT-0001/timeline` (con un token con rol Admin o
  Auditor y scope `OT.Read`) devuelve el evento.
- Reenviar el mismo mensaje (mismo `eventId`): el log muestra
  `ya registrado, se descarta el duplicado` y no aparece dos veces en el
  timeline.
- Producir `{no es json` en el mismo tópico: aparece en
  `digitalfix.workorders.events.DLT` sin pasar por reintentos.

Variables de entorno para apuntar a otro clúster:

| Variable | Por defecto |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093,localhost:9094` |

---

## Pendiente

- Segundo consumidor para `digitalfix.audit.events` en otro servicio (notify
  o report), que es quien le daría uso real al registro que audit publica.
- Pantalla de Auditoría en `DigitalFix-Frontend` (hoy `audit.ts` es un
  placeholder).
- Despliegue en AWS: la instancia necesita memoria suficiente para 3
  Zookeeper + 3 brokers, y falta la ruta en el API Gateway.
- Migraciones de esquema en vez de `ddl-auto: update`.

---

## Nota sobre Spring Boot 4 / Spring Kafka 4

Este proyecto usa Spring Boot 4.0.8 y Spring Kafka 4.0.7. Dos detalles que
cambiaron de nombre o ubicación respecto de versiones anteriores, por si se
regenera el `pom.xml` desde Spring Initializr:

- `ExponentialBackOffWithMaxRetries` vive en
  `org.springframework.kafka.support`, no en `...listener` donde está
  `DefaultErrorHandler`.
- El `ObjectMapper` se declara a mano en `JacksonConfig` en vez de depender de
  la autoconfiguración de Jackson, por la misma razón que
  `RabbitProducerConfig` lo hace en `ms-digitalfix-workorders`.
