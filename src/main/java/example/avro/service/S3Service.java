package example.avro.service;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import example.avro.exception.AvroException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.ByteBuffer;

public class S3Service {

    private static Logger LOGGER = LoggerFactory.getLogger(S3Service.class);

    private AmazonS3 s3Client;

    public S3Service(){
        this.s3Client = initAmazonS3();
    }

    private static AmazonS3 initAmazonS3() {
        AmazonS3 amazonS3 = null;

        try {
            amazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
        } catch (Exception var3) {
            LOGGER.error("Error occurred while initializing S3 Client: {}", var3.getMessage());
        }

        return amazonS3;
    }

    public void putFile(String bucketName, String folderName, String fileName, byte[] bytes) throws AvroException {
        if (s3Client == null) {
            throw new RuntimeException("S3 Client is null.");
        }
        String key = folderName + fileName;
        try {
            LOGGER.debug("Trying to upload file...");

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(bytes.length);

            PutObjectRequest request = new PutObjectRequest(bucketName, key, new ByteArrayInputStream(bytes), objectMetadata);
            s3Client.putObject(request);
        } catch (Exception e) {
            LOGGER.error("Error occurred while putting file in S3 bucket: {}", e.getMessage());
            throw new AvroException("Error occurred while putting file in S3 bucket: " + e.getMessage());
        }
        LOGGER.info("File {} successfully uploaded", key);
    }
}
