package ca.stevenskelton.aws.lambda.s3xz;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogger implements LambdaLogger {
    private static final Logger logger = LoggerFactory.getLogger(TestLogger.class);

    public TestLogger() {
    }

    public void log(String message) {
        logger.info(message);
    }

    public void log(byte[] message) {
        logger.info(new String(message));
    }
}