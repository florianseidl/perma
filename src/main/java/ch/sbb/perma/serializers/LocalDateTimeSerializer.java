/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Bytes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;

public class LocalDateTimeSerializer implements KeyOrValueSerializer<LocalDateTime> {
    private final int lengthOfLocalDateAsBytes;

    public LocalDateTimeSerializer() {
        lengthOfLocalDateAsBytes = LOCAL_DATE.toByteArray(LocalDate.MAX).length;
    }

    @Override
    public byte[] toByteArray(LocalDateTime localDateTime) {
        return Bytes.concat(
                LOCAL_DATE.toByteArray(localDateTime.toLocalDate()),
                LOCAL_TIME.toByteArray(localDateTime.toLocalTime())
        );
    }

    @Override
    public LocalDateTime fromByteArray(byte[] bytes) {
        byte[] timeBytes = Arrays.copyOfRange(bytes, lengthOfLocalDateAsBytes, bytes.length);
        return LocalDateTime.of(
                LOCAL_DATE.fromByteArray(bytes),
                LOCAL_TIME.fromByteArray(timeBytes));
    }
}
