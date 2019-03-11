package example.avro;

import example.avro.object.User;
import example.avro.exception.AvroException;
import example.avro.service.S3Service;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static example.avro.service.AvroService.*;

public class Main {

    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static String bucketName = "avro.files";
    private static String folderName = "Uploaded/";

    private static S3Service service = new S3Service();

    public static void main(String args[]) {
        try {
            List<User> listOfUsers = Arrays.asList(createUser("Mariana", 21, "violet"));
            String fileName = createUniqueFileName();

            byte[] bytes = createUserAvroFile(listOfUsers);
            service.putFile(bucketName, folderName, fileName, bytes);
            decodeAvroData(bytes);
        } catch (AvroException e) {
           LOGGER.error("Error occurred: {}", e.getMessage());
        }

    }

}
