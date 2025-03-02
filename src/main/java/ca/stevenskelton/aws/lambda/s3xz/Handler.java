package ca.stevenskelton.aws.lambda.s3xz;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class Handler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        final LambdaLogger logger = context.getLogger();
        try {
            final ObjectMapper objectMapper = new ObjectMapper();
            final Request request = objectMapper.readValue(event.getBody(), Request.class);

            final S3Client s3Client = S3Client.builder().build();
            final ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(request.bucketName())
                    .prefix(request.folder())
                    .build();

            final ListObjectsV2Iterable listObjectsV2Iterable = s3Client.listObjectsV2Paginator(listObjectsV2Request);
            final List<S3Object> folderContents = listObjectsV2Iterable.stream().flatMap(r -> r.contents().stream()).toList();
            final long folderBytes = folderContents.stream().mapToLong(S3Object::size).sum();
            logger.log("Found " + folderContents.size() + " objects, total " + folderBytes + " bytes");
            final File tarFile = File.createTempFile("prefix-", "-suffix");
            tarFile.deleteOnExit();

            final XZTarFile xzTarFile = new XZTarFile(tarFile);
            final List<ObjectIdentifier> deleteObjectIndentifiers = new ArrayList<>(folderContents.size());

            for (S3Object s3Object : folderContents) {
                logger.log(s3Object.key() + ", " + s3Object.size() + " bytes");
                final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(request.bucketName())
                        .key(s3Object.key())
                        .build();
                final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
                xzTarFile.putEntry(s3Object.key(), s3Object.size(), responseInputStream);
                deleteObjectIndentifiers.add(ObjectIdentifier.builder().key(s3Object.key()).build());
            }
            final long tarFileCRC32 = xzTarFile.getCRC32();

            final String tarFileBase64CRC32 = Base64.getEncoder().encodeToString(BigInteger.valueOf(tarFileCRC32).toByteArray());
            logger.log("Created " + tarFile.getAbsolutePath() + ", " + Files.size(tarFile.toPath()) +
                    "  bytes (" + folderBytes + " compressed), CRC32=" + tarFileBase64CRC32);
            final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(request.bucketName())
                    .key(request.outputFileName())
                    .contentType("application/tar+xz")
                    .build();
            final PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, RequestBody.fromFile(tarFile));

            if (tarFileBase64CRC32.equals(putObjectResponse.checksumCRC32())) {
                logger.log("Uploaded to S3 " + request.outputFileName() + " in bucket " + request.bucketName());
                if (request.deleteAfterCompress()) {
                    final DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                            .bucket(request.bucketName())
                            .delete(Delete.builder().objects(deleteObjectIndentifiers).build())
                            .build();
                    s3Client.deleteObjects(deleteObjectsRequest);
                }
                final Response response = new Response(
                        request.outputFileName(),
                        putObjectResponse.size(),
                        folderContents.size(),
                        folderBytes,
                        request.deleteAfterCompress()
                );
                return new APIGatewayV2HTTPResponse(
                        200, //statusCode
                        Map.of(), //headers
                        Map.of(), //multiValueHeaders
                        List.of(), //cookies
                        objectMapper.writeValueAsString(response), //body
                        false //isBase64Encoded
                );
            } else {
                final RuntimeException ex = new RuntimeException("Upload failed, CRC32 " + putObjectResponse.checksumCRC32() + " but " + tarFileBase64CRC32 + "expected.");
                logger.log(ex.getMessage());
                throw ex;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    private InputStream getObject(S3Client s3Client, String bucket, String key) {
//        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
//                .bucket(bucket)
//                .key(key)
//                .build();
//        return s3Client.getObject(getObjectRequest);
//    }

//    private void putObject(S3Client s3Client, ByteArrayOutputStream outputStream,
//                           String bucket, String key, String imageType, LambdaLogger logger) {
//        Map<String, String> metadata = new HashMap<>();
//        metadata.put("Content-Length", Integer.toString(outputStream.size()));
//        if (JPG_TYPE.equals(imageType)) {
//            metadata.put("Content-Type", JPG_MIME);
//        } else if (PNG_TYPE.equals(imageType)) {
//            metadata.put("Content-Type", PNG_MIME);
//        }
//
//        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
//                .bucket(bucket)
//                .key(key)
//                .metadata(metadata)
//                .build();
//
//        // Uploading to S3 destination bucket
//        logger.log("Writing to: " + bucket + "/" + key);
//        try {
//            s3Client.putObject(putObjectRequest,
//                    RequestBody.fromBytes(outputStream.toByteArray()));
//        }
//        catch(AwsServiceException e)
//        {
//            logger.log(e.awsErrorDetails().errorMessage());
//            System.exit(1);
//        }
//    }

}