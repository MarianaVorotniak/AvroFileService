package example.avro.service;

import example.avro.object.User;
import example.avro.exception.AvroException;
import org.apache.log4j.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class AvroServiceTest {

    @InjectMocks
    private static AvroService service;
    @Mock
    private static User user;
    @Mock
    private static List<User> listOfUsers;
    @Mock
    private Appender mockAppender;

    private static Logger logger;
    private static ByteArrayOutputStream out;
    private static Appender appender;

    private static String testFilePath1 = "C:\\Users\\mvoro\\IdeaProjects\\AvroFileService\\src\\test\\java\\resources\\";
    private static String testFilePath2 = "C:\\Users\\mvoro\\IdeaProjects\\AvroFileService\\src\\test\\java\\resources\\testUsers.avro";

    @Before
    public void init() {
        user = new User("Mariana", 21, "violet");
        listOfUsers = Arrays.asList(new User("User1", 25, "red"),
                new User("User2", 26, "green"));

        LogManager.getRootLogger().addAppender(mockAppender);

        Layout layout = new SimpleLayout();

        logger = Logger.getLogger(AvroService.class);
        out = new ByteArrayOutputStream();
        appender = new WriterAppender(layout, out);
        logger.addAppender(appender);
    }

    @Test
    public void createUserTest() throws AvroException {
        User testUser1 = service.createUser("Mariana", 21, "violet");
        User testUser2 = service.createUser("Diana", 21, "violet");
        assertEquals(testUser1, user);
        assertNotEquals(testUser2, user);
    }


//    @Test
//    public void deserializeUsersTest() throws AvroException {
//        service.deserializeUserAvroFile(testFilePath2);
//
//        String logMsg = out.toString();
//        assertNotNull(logMsg);
//    }

    @After
    public void destroy() {
        logger.removeAppender(appender);
    }


}
