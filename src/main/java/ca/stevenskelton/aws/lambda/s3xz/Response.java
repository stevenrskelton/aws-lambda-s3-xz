package ca.stevenskelton.aws.lambda.s3xz;

public record Response(
        String xzFileName,
        long xzFileSize,
        int inputFileCount,
        long inputFileSize,
        boolean deletedInputFiles
) {
}
