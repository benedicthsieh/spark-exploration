import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Main {
  private static String path = "/Users/benedicthsieh/git/slack-java/emails/enron_with_categories/";

  public static void main(String[] args) {
    String logPath = path + "*/*"; //+ "1/3111.txt"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.wholeTextFiles(logPath).values();
    JavaRDD<MimeMessage> parsedData = logData.map(Main::stringToMimeMessage).filter(m -> m != null);

    long recipients = parsedData
        .filter(m -> m.getAllRecipients() != null && m.getAllRecipients().length > 1)
        .count();
    System.out.println("Emails with multiple recipients: " + recipients);
  }

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