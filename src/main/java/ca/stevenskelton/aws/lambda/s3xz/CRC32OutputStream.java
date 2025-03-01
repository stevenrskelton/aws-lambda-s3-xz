package ca.stevenskelton.aws.lambda.s3xz;

import org.jetbrains.annotations.NotNull;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;

public class CRC32OutputStream extends FilterOutputStream {

    private final CRC32 checksum = new CRC32();

    public CRC32OutputStream(OutputStream out) {
        super(out);
    }

    public long crc32() {
        return checksum.getValue();
    }

    @Override
    public void write(int b) throws IOException {
        checksum.update(b);
        super.write(b);
    }

    @Override
    public void write(@NotNull byte[] b) throws IOException {
        checksum.update(b);
        super.write(b);
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
        checksum.update(b, off, len);
        super.write(b, off, len);
    }
}
