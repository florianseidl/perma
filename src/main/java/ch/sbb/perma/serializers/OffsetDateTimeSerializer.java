/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Bytes;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class OffsetDateTimeSerializer implements KeyOrValueSerializer<OffsetDateTime> {
    private final int lengthOfLocalDateTimeAsBytes;

    public OffsetDateTimeSerializer() {
        lengthOfLocalDateTimeAsBytes = LOCAL_DATE_TIME.toByteArray(LocalDateTime.MAX).length;
    }

    @Override
    public byte[] toByteArray(OffsetDateTime offsetDateTime) {
        return Bytes.concat(
                LOCAL_DATE_TIME.toByteArray(offsetDateTime.toLocalDateTime()),
                STRING.toByteArray(offsetDateTime.getOffset().getId())
        );
    }

    @Override
    public OffsetDateTime fromByteArray(byte[] bytes) {
        byte[] offsetBytes = Arrays.copyOfRange(bytes, lengthOfLocalDateTimeAsBytes, bytes.length);
        return OffsetDateTime.of(
                LOCAL_DATE_TIME.fromByteArray(bytes),
                ZoneOffset.of(STRING.fromByteArray(offsetBytes)));
    }

}
