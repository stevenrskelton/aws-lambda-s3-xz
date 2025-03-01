package ca.stevenskelton.aws.lambda.s3xz;

import org.jetbrains.annotations.Nullable;

import java.util.List;


public record Request(
        String bucketName,
        @Nullable String folder,
        List<String> files,
        String outputFileName,
        boolean deleteAfterCompress
) {
}
