/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.nio.charset.Charset;

/**
 * Serialize Non-Null-Strings.
 * <p>
 *     The serialized form of Strings is UTF-8.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class StringSerializer implements KeyOrValueSerializer<String> {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public StringSerializer() {
    }

    @Override
    public byte[] toByteArray(String string) {
        return string.getBytes(UTF_8);
    }

    @Override
    public String fromByteArray(byte[] bytes) {
        return new String(bytes, UTF_8);
    }
}
