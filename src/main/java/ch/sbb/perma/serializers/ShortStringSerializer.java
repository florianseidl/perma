/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

/**
 * Serialize short non-null-Strings to 2 bytes a character. Ideal for short strings of to 4 or 5 characters.
 * <p>
 *     The serialized form of Strings is UTF-16.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class ShortStringSerializer implements KeyOrValueSerializer<String> {
    @Override
    public byte[] toByteArray(String string) {
        return string.getBytes();
    }

    @Override
    public String fromByteArray(byte[] bytes) {
        return new String(bytes);
    }
}
