/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.nio.charset.Charset;
import java.util.Optional;

/**
 * A serializer for nullable strings.
 * <p>
 *     Uses optional as null is not allowed in ImmutableMap and ConcurrentMap.
 *     The persisted for of an empty optional is null as the binary PerMa record format supports null keys and values.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class OptionalStringSerializer implements KeyOrValueSerializer<Optional<String>> {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    OptionalStringSerializer() {
    }

    @Override
    public byte[] toByteArray(Optional<String> optionalString) {
        return optionalString
                .map(string -> STRING.toByteArray(string))
                .orElse(null);
    }

    @Override
    public Optional<String> fromByteArray(byte[] bytes) {
        if(bytes == null) {
            return Optional.empty();
        }
        return Optional.of(STRING.fromByteArray(bytes));
    }
}
