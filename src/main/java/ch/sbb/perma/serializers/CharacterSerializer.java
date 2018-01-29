/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Chars;

/**
 * Serialize single characters to 2 bytes.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2017.
 */
public class CharacterSerializer implements KeyOrValueSerializer<Character> {
    @Override
    public byte[] toByteArray(Character charValue) {
        return Chars.toByteArray(charValue);
    }

    @Override
    public Character fromByteArray(byte[] bytes) {
        return Chars.fromByteArray(bytes);
    }
}
