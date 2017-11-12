/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import org.javatuples.Triplet;

/**
 * Serializer and deserializer for javatuples Triplets.
 * <p>
 *     Requires 3 serializers for the 3 values.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public class TripletSerializer<A,B,C> extends TupleSerializer<Triplet<A,B,C>> {

    public TripletSerializer(KeyOrValueSerializer<A> value0Serializer,
                             KeyOrValueSerializer<B> value1Serializer,
                             KeyOrValueSerializer<C> value2Serializer) {
        super(new KeyOrValueSerializer[]{value0Serializer, value1Serializer,value2Serializer});
    }

    @Override
    @SuppressWarnings("unchecked")
    Triplet<A,B,C> createFrom(Object[] values) {
        return new Triplet(values[0], values[1],values[2]);
    }
}
