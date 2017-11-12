/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Chars;

/**
 * Serialize single characters to 2 bytes.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class CharSerializer implements KeyOrValueSerializer<Character> {
    @Override
    public byte[] toByteArray(Character charValue) {
        return Chars.toByteArray(charValue);
    }

    @Override
    public Character fromByteArray(byte[] bytes) {
        return Chars.fromByteArray(bytes);
    }
}
