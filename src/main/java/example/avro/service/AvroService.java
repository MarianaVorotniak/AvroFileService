package example.avro.service;

import example.avro.object.User;
import example.avro.exception.AvroException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class AvroService {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroService.class);

    public static User createUser(String name, Integer favouriteNumber, String favouriteColour) throws AvroException {
        try {
            return new User(name, favouriteNumber, favouriteColour);
        } catch (Exception e) {
            LOGGER.error("Error occurred while creating new User: {}", e.getMessage());
            throw new AvroException("Error occurred while creating new User: " + e.getMessage());
        }
    }

    public static byte[] createUserAvroFile(List<User> users) throws AvroException {
        byte[] data;
        try {
            DatumWriter<User> outputDatumWriter = new SpecificDatumWriter<>(User.class);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

            for (User user : users) {
                LOGGER.debug("Serializing user {}...", user);
                outputDatumWriter.write(user, encoder);
                LOGGER.info("User {} successfully serialized!", user);
            }
            encoder.flush();
            data = baos.toByteArray();

            LOGGER.info("Serialized data: " + data);
        } catch (Exception e) {
            LOGGER.error("Error occurred while creating new AVRO file: {}", e.getMessage());
            throw new AvroException("Error occurred while creating AVRO file: " + e.getMessage());
        }
            return data;
    }

    public static void decodeAvroData(byte[] byteData)  {
        DatumReader<GenericRecord> datumReaderUser = new GenericDatumReader<>(User.getClassSchema());
        GenericRecord genericRecord = null;
        try {
            genericRecord = datumReaderUser.read(null, DecoderFactory.get().binaryDecoder(byteData, null));
        } catch (IOException e) {
            LOGGER.error("Error occurred decoding avro data: " + e.getMessage());
        }
        LOGGER.info("Decoded avro data: " + genericRecord);
    }

    public static void deserializeUserAvroFile(String filePath) throws AvroException {
        try {
            DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
            DataFileReader<User> dataFileReader = new DataFileReader<>(new File(filePath), userDatumReader);
            User user = null;
            while (dataFileReader.hasNext()) {
                LOGGER.debug("Deserializing user {}...", user);
                user = dataFileReader.next(user);
                LOGGER.info("Deserialized user: {}", user);
            }
        }catch (Exception e) {
            LOGGER.error("Error occurred while deserializing AVRO file: {}", e.getMessage());
            throw new AvroException("Error occurred while deserializing AVRO file: " + e.getMessage());
        }
    }

    public static String createUniqueFileName() {
        LocalDateTime now = LocalDateTime.now();
        String strNow = now.format(DateTimeFormatter.ofPattern("hhmmss"));
        return strNow + "user.avro";
    }
}
