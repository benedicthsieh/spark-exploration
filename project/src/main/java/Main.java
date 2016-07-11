import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import javax.mail.internet.MimeMessage;

public class Main {


  public static void main(String[] args) {
    //String logPath = testpath + "*/*.txt"; //+ "1/3111.txt"; // Should be some file on your system
    String logPath = Utils.path + "*/*.txt";
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.wholeTextFiles(logPath).values();
    JavaRDD<MimeMessage> parsedData = logData.map(Utils::stringToMimeMessage).filter(m -> m != null);

    //long recipients = parsedData
    //    .filter(m -> m.getAllRecipients() != null && m.getAllRecipients().length > 1)
    //    .count();
    long recipients = parsedData.count();
    System.out.println("Emails with multiple recipients: " + recipients);
  }

}