import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by benedicthsieh on 7/11/16.
 */
public class Utils {
  static String path = "/Users/benedicthsieh/git/slack-java/emails/enron_with_categories/*/*.txt";
  static String testpath = "/Users/benedicthsieh/git/slack-java/emails/test/*/*.txt";

  public static MimeMessage stringToMimeMessage(String content) {
    Session s = Session.getDefaultInstance(new Properties());
    InputStream is = new ByteArrayInputStream(content.getBytes());
    try {
      return new MimeMessage(s, is);
    } catch (MessagingException e) {
      System.err.println("ERROR parsing: " + content);
      return null;
    }
  }
}
