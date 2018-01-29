/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

/**
 * Serialize a string array.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public class StringArraySerializer extends ObjectArraySerializer<String> {

    public StringArraySerializer() {
        super(String.class, STRING);
    }
}
