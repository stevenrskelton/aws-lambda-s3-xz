package ca.stevenskelton.aws.lambda.s3xz;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jetbrains.annotations.NotNull;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZ;
import org.tukaani.xz.XZOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;

public class XZTarFile implements AutoCloseable {

    public final File file;

    private final int bufferSize = 8048;

    private final FileOutputStream fileOutputStream;
    private final CRC32OutputStream crc32OutputStream;
    private final XZOutputStream xzOutputStream;
    private final TarArchiveOutputStream tarOutputStream;

    public XZTarFile(@NotNull File file) throws IOException {
        this(file, 9);
    }

    public XZTarFile(@NotNull File file, int compressionLevel) throws IOException {
        this.file = file;
        this.fileOutputStream = new FileOutputStream(file);
        this.crc32OutputStream = new CRC32OutputStream(fileOutputStream);
        this.xzOutputStream = new XZOutputStream(crc32OutputStream, new LZMA2Options(compressionLevel), XZ.CHECK_SHA256);
        this.tarOutputStream = new TarArchiveOutputStream(xzOutputStream);
    }

    @Override
    public void close() throws Exception {
        tarOutputStream.close();
        xzOutputStream.close();
        crc32OutputStream.close();
        fileOutputStream.close();
    }

    public long crc32() throws IOException {
        tarOutputStream.flush();
        xzOutputStream.flush();
        crc32OutputStream.flush();
        fileOutputStream.flush();
        return crc32OutputStream.crc32();
    }

    public void putEntry(@NotNull String name, @NotNull FilterInputStream body) throws IOException {
        tarOutputStream.putArchiveEntry(new TarArchiveEntry(name));
        final byte[] buf = new byte[bufferSize];
        int len;
        while ((len = body.read(buf)) > 0) {
            tarOutputStream.write(buf, 0, len);
        }
        tarOutputStream.closeArchiveEntry();
    }
}
