package com.example.demo_gateway_ai.mcp;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.async.ByteArrayFeeder;
import tools.jackson.core.json.JsonFactory;

import java.util.function.Consumer;

/**
 * Framing SSE + extraction JSON-RPC incrémentale côté réponse (EF-6).
 *
 * <p>Machine à états par flux de réponse : découpe les lignes ({@code data:}, {@code event:},
 * {@code id:}, commentaires {@code :}) et détecte la fin d'évènement (ligne vide), en tolérant
 * les coupures de chunk au milieu d'une ligne. Le contenu des lignes {@code data:} est poussé
 * au fil de l'eau dans le parser non-bloquant de Jackson 3
 * ({@code JsonFactory.createNonBlockingByteArrayParser}) : un évènement coupé entre deux chunks
 * est parsé sans retenir les octets du flux — seuls l'état interne du parser et les champs
 * extraits sont conservés.
 *
 * <p>L'accumulation est bornée à {@code maxEventBytes} par évènement, limite appliquée par le
 * composant lui-même (les {@code StreamReadConstraints} ne sont pas fiables avec les parsers
 * async de Jackson 3.x, cf. GHSA-2m67-wjpj-xhg9) : au-delà, l'évènement est abandonné (parser
 * réinitialisé, WARN) et le framing continue jusqu'à la frontière suivante.
 *
 * <p>Non thread-safe : une instance par flux de réponse (Reactor sérialise les {@code onNext}).
 */
@Slf4j
final class SseEventAssembler {

    private static final JsonFactory JSON_FACTORY = JsonFactory.builder().build();
    private static final byte[] DATA_LINE_SEPARATOR = {'\n'};
    private static final int MAX_FIELD_NAME_CHARS = 16;

    private enum State {
        /** Début de ligne : ligne vide = fin d'évènement, {@code :} = commentaire. */
        LINE_START,
        /** Accumulation du nom de champ SSE jusqu'au {@code :} ou fin de ligne. */
        FIELD_NAME,
        /** Juste après {@code data:} : un éventuel espace de tête à ignorer. */
        DATA_VALUE_START,
        /** Contenu d'une ligne {@code data:}, poussé directement dans le parser. */
        DATA_VALUE,
        /** Ligne sans intérêt ({@code event:}, {@code id:}, commentaire) : ignorée jusqu'à la fin de ligne. */
        IGNORED_LINE
    }

    private final long maxEventBytes;
    private final Consumer<McpResponseMessage> onMessage;
    private final JsonRpcMessageExtractor extractor = new JsonRpcMessageExtractor();
    private final StringBuilder fieldName = new StringBuilder(MAX_FIELD_NAME_CHARS);

    private State state = State.LINE_START;
    private boolean skipLf;
    private boolean eventHasData;
    private boolean discardingEvent;
    private long eventBytes;
    private JsonParser parser;
    private ByteArrayFeeder feeder;

    SseEventAssembler(long maxEventBytes, Consumer<McpResponseMessage> onMessage) {
        this.maxEventBytes = maxEventBytes;
        this.onMessage = onMessage;
    }

    /**
     * Observe un chunk du flux de réponse. Le tableau n'est pas conservé après l'appel ;
     * les octets utiles sont consommés immédiatement par le parser non-bloquant.
     */
    void onChunk(byte[] chunk, int offset, int length) {
        int i = offset;
        int end = offset + length;
        while (i < end) {
            byte b = chunk[i];
            if (skipLf) {
                skipLf = false;
                if (b == '\n') {
                    i++;
                    continue;
                }
            }
            switch (state) {
                case LINE_START -> {
                    if (b == '\n' || b == '\r') {
                        skipLf = b == '\r';
                        endOfEvent();
                        i++;
                    } else if (b == ':') {
                        state = State.IGNORED_LINE;
                        i++;
                    } else {
                        fieldName.setLength(0);
                        state = State.FIELD_NAME;
                    }
                }
                case FIELD_NAME -> {
                    if (b == ':') {
                        if (isDataField()) {
                            startDataLine();
                            state = State.DATA_VALUE_START;
                        } else {
                            state = State.IGNORED_LINE;
                        }
                        i++;
                    } else if (b == '\n' || b == '\r') {
                        // champ sans ":" : équivaut à une valeur vide ("data" seul = ligne data vide)
                        if (isDataField()) {
                            startDataLine();
                        }
                        skipLf = b == '\r';
                        state = State.LINE_START;
                        i++;
                    } else {
                        if (fieldName.length() < MAX_FIELD_NAME_CHARS) {
                            fieldName.append((char) (b & 0xFF));
                            i++;
                        } else {
                            state = State.IGNORED_LINE;
                        }
                    }
                }
                case DATA_VALUE_START -> {
                    if (b == ' ') {
                        i++;
                    }
                    state = State.DATA_VALUE;
                }
                case DATA_VALUE -> {
                    int lineEnd = i;
                    while (lineEnd < end && chunk[lineEnd] != '\n' && chunk[lineEnd] != '\r') {
                        lineEnd++;
                    }
                    feed(chunk, i, lineEnd);
                    if (lineEnd < end) {
                        skipLf = chunk[lineEnd] == '\r';
                        state = State.LINE_START;
                        lineEnd++;
                    }
                    i = lineEnd;
                }
                case IGNORED_LINE -> {
                    while (i < end && chunk[i] != '\n' && chunk[i] != '\r') {
                        i++;
                    }
                    if (i < end) {
                        skipLf = chunk[i] == '\r';
                        state = State.LINE_START;
                        i++;
                    }
                }
            }
        }
    }

    /** À appeler à la terminaison du flux de réponse : libère le parser en cours. */
    void close() {
        closeParser();
        discardingEvent = false;
        eventHasData = false;
        eventBytes = 0;
    }

    private boolean isDataField() {
        return fieldName.length() == 4 && "data".contentEquals(fieldName);
    }

    /** Début d'une ligne {@code data:} : prépare le parser et joint les lignes successives par {@code \n}. */
    private void startDataLine() {
        if (discardingEvent) {
            return;
        }
        if (parser == null) {
            try {
                parser = JSON_FACTORY.createNonBlockingByteArrayParser(ObjectReadContext.empty());
                feeder = (ByteArrayFeeder) parser.nonBlockingInputFeeder();
                extractor.reset();
            } catch (JacksonException e) {
                abandonEvent("création du parser impossible: " + e.getMessage());
                return;
            }
        }
        if (eventHasData) {
            feed(DATA_LINE_SEPARATOR, 0, 1);
        }
        eventHasData = true;
    }

    private void feed(byte[] data, int from, int to) {
        if (discardingEvent || parser == null || to <= from) {
            return;
        }
        eventBytes += to - from;
        if (eventBytes > maxEventBytes) {
            abandonEvent("évènement SSE > " + maxEventBytes + " octets (mcp.listener.max-sse-event-bytes)");
            return;
        }
        try {
            feeder.feedInput(data, from, to);
            drainTokens();
        } catch (JacksonException e) {
            abandonEvent("payload data: non parsable en JSON: " + e.getMessage());
        }
    }

    private void drainTokens() {
        JsonToken token;
        while ((token = parser.nextToken()) != null && token != JsonToken.NOT_AVAILABLE) {
            extractor.onToken(parser, token);
        }
    }

    /** Fin d'évènement (ligne vide) : termine le parsing et publie le message extrait. */
    private void endOfEvent() {
        if (!discardingEvent && eventHasData && parser != null) {
            try {
                feeder.endOfInput();
                drainTokens();
                McpResponseMessage message = extractor.toMessage();
                if (message != null) {
                    onMessage.accept(message);
                }
            } catch (JacksonException e) {
                log.warn("Évènement SSE abandonné (JSON incomplet ou invalide): {}", e.getMessage());
            }
        }
        closeParser();
        discardingEvent = false;
        eventHasData = false;
        eventBytes = 0;
    }

    private void abandonEvent(String reason) {
        log.warn("Évènement SSE abandonné, le flux continue d'être proxifié: {}", reason);
        closeParser();
        discardingEvent = true;
    }

    private void closeParser() {
        if (parser != null) {
            try {
                parser.close();
            } catch (JacksonException e) {
                log.debug("Fermeture du parser SSE: {}", e.getMessage());
            }
            parser = null;
            feeder = null;
        }
        extractor.reset();
    }

    /**
     * Extraction à plat des champs JSON-RPC de réponse ({@code $.result}, {@code $.error},
     * {@code id}) à partir du flux de tokens, sans matérialiser l'arbre JSON.
     */
    private static final class JsonRpcMessageExtractor {

        private int depth;
        private String rootField;
        private String errorField;
        private String id;
        private boolean hasResult;
        private boolean hasError;
        private Integer errorCode;
        private String errorMessage;
        private boolean rootCompleted;

        void onToken(JsonParser parser, JsonToken token) {
            switch (token) {
                case START_OBJECT, START_ARRAY -> {
                    depth++;
                    if (depth == 2 && rootField != null) {
                        switch (rootField) {
                            case "result" -> hasResult = true;
                            case "error" -> hasError = true;
                            default -> { }
                        }
                    }
                }
                case END_OBJECT, END_ARRAY -> {
                    depth--;
                    if (depth == 0) {
                        rootCompleted = true;
                    }
                }
                case PROPERTY_NAME -> {
                    if (depth == 1) {
                        rootField = parser.currentName();
                    } else if (depth == 2 && "error".equals(rootField)) {
                        errorField = parser.currentName();
                    }
                }
                case NOT_AVAILABLE -> { }
                default -> { // valeurs scalaires
                    if (depth == 1 && rootField != null) {
                        switch (rootField) {
                            case "id" -> id = parser.getValueAsString();
                            case "result" -> hasResult = true;
                            case "error" -> hasError = token != JsonToken.VALUE_NULL;
                            default -> { }
                        }
                    } else if (depth == 2 && "error".equals(rootField) && errorField != null) {
                        switch (errorField) {
                            case "code" -> {
                                if (token == JsonToken.VALUE_NUMBER_INT) {
                                    errorCode = parser.getIntValue();
                                }
                            }
                            case "message" -> errorMessage = parser.getValueAsString();
                            default -> { }
                        }
                    }
                }
            }
        }

        McpResponseMessage toMessage() {
            if (!rootCompleted || (id == null && !hasResult && !hasError)) {
                return null;
            }
            return new McpResponseMessage(id, hasResult, hasError, errorCode, errorMessage);
        }

        void reset() {
            depth = 0;
            rootField = null;
            errorField = null;
            id = null;
            hasResult = false;
            hasError = false;
            errorCode = null;
            errorMessage = null;
            rootCompleted = false;
        }
    }
}
