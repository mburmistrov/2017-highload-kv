package ru.mail.polis.mburmistrov.util;

import com.google.common.io.ByteStreams;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public class DataReader {

    private static final int BUFFER_SIZE = 1024;

    public static byte[] readData(@NotNull InputStream is) throws IOException {
        return ByteStreams.toByteArray(is);
    }
}
