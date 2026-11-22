package cl.duoc.ms_digitalfix_audit.exception;

/**
 * Fallo transitorio durante el consumo: el evento era valido pero el intento
 * no prospero (tipicamente la base de datos no respondio). Reintentar tiene
 * sentido porque la causa puede desaparecer sola.
 *
 * Mismo vocabulario que ms-digitalfix-notify usa para RabbitMQ: la distincion
 * recuperable/no recuperable es la misma, solo cambia el transporte.
 */
public class ErrorRecuperableException extends RuntimeException {

    public ErrorRecuperableException(String message) {
        super(message);
    }

    public ErrorRecuperableException(String message, Throwable cause) {
        super(message, cause);
    }
}
