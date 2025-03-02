package ca.stevenskelton.aws.lambda.s3xz;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jetbrains.annotations.NotNull;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZ;
import org.tukaani.xz.XZOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class XZTarFile implements AutoCloseable {

    public final File file;

    private static final int bufferSize = 8048;
    
    private final FileOutputStream fileOutputStream;
    private final CheckedOutputStream checkedOutputStream;
    private final XZOutputStream xzOutputStream;
    private final TarArchiveOutputStream tarOutputStream;

    public XZTarFile(@NotNull File file) throws IOException {
        this(file, 9);
    }

    public XZTarFile(@NotNull File file, int compressionLevel) throws IOException {
        this.file = file;
        this.fileOutputStream = new FileOutputStream(file);
        this.checkedOutputStream = new CheckedOutputStream(fileOutputStream, new CRC32());
        this.xzOutputStream = new XZOutputStream(checkedOutputStream, new LZMA2Options(compressionLevel), XZ.CHECK_SHA256);
        this.tarOutputStream = new TarArchiveOutputStream(xzOutputStream);
    }

    @Override
    public void close() throws Exception {
        getCRC32();
        tarOutputStream.close();
        xzOutputStream.close();
        checkedOutputStream.close();
        fileOutputStream.close();
    }

    public long getCRC32() throws IOException {
        tarOutputStream.flush();
        xzOutputStream.flush();
        checkedOutputStream.flush();
        fileOutputStream.flush();
        return checkedOutputStream.getChecksum().getValue();
    }

    public void putEntry(@NotNull String name, long fileSize, @NotNull InputStream body) throws IOException {
        final TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(name);
        tarArchiveEntry.setSize(fileSize);
        tarOutputStream.putArchiveEntry(tarArchiveEntry);
        final byte[] buf = new byte[bufferSize];
        int len;
        while ((len = body.read(buf)) > 0) {
            tarOutputStream.write(buf, 0, len);
        }
        tarOutputStream.closeArchiveEntry();
    }
}
