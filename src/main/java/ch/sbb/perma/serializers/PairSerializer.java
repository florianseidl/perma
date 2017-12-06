/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import org.javatuples.Pair;

/**
 * Serializer and deserializer for javatuples Pairs.
 * <p>
 *     Requires 2 serializers for the 2 values. They may not seriaize non-null values to null.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public class PairSerializer<A,B> extends TupleSerializer<Pair<A,B>> {

    public PairSerializer(KeyOrValueSerializer<A> value0Serializer,
                          KeyOrValueSerializer<B> value1Serializer) {
        super(value0Serializer, value1Serializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    Pair<A,B> createFrom(Object[] values) {
        return new Pair(values[0], values[1]);
    }
}
