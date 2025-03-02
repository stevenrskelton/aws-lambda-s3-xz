package ca.stevenskelton.aws.lambda.s3xz;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XZTarFileTest {

    InputStream getResource(String name) {
        return getClass().getClassLoader().getResourceAsStream(name);
    }

    long getCRC32(File file) throws IOException {
        if (!file.exists()) return -1;
        CRC32 crc = new CRC32();
        final byte[] buf = new byte[8048];
        try (InputStream in = new CheckedInputStream(new FileInputStream(file), crc)) {
            while (in.read(buf) > 0) ;
        }
        return crc.getValue();
    }

    @Test
    void invokeTest() throws Exception {
        final Path tempDirectory = Files.createTempDirectory(getClass().getName());
        final File tarFile = new File(tempDirectory + "/output.tar.xz");
        final XZTarFile xzTarFile = new XZTarFile(tarFile);
        xzTarFile.putEntry("bird.jpg", 87940, getResource("bird.jpg"));
        xzTarFile.putEntry("boardwalk.jpg", 46008, getResource("boardwalk.jpg"));
        xzTarFile.putEntry("goose.jpg", 83694, getResource("goose.jpg"));
        xzTarFile.close();

        final ProcessBuilder builder = new ProcessBuilder();
        builder.directory(tarFile.getParentFile());
        builder.command("tar", "--extract", "--xz", "--file=" + tarFile.getName());
        final Process process = builder.start();
        process.waitFor(1, TimeUnit.SECONDS);
        assertEquals(0, process.exitValue());

        final File bird = new File(tempDirectory + "/bird.jpg");
        assertEquals(87940, Files.size(bird.toPath()));
        assertEquals(4258644370L, getCRC32(bird));

        final File boardwalk = new File(tempDirectory + "/boardwalk.jpg");
        assertEquals(46008, Files.size(boardwalk.toPath()));
        assertEquals(1650269992L, getCRC32(boardwalk));

        final File goose = new File(tempDirectory + "/goose.jpg");
        assertEquals(83694, Files.size(goose.toPath()));
        assertEquals(2283001591L, getCRC32(goose));
    }

}
