import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.mail.internet.MimeMessage;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by benedicthsieh on 7/11/16.
 */
public class DirectAndBroadcastMain {
  public static void main(String[] args) {
    String logPath = Utils.emailsPath;
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.wholeTextFiles(logPath).values();
    JavaRDD<MimeMessage> parsedData = logData.map(Utils::stringToMimeMessage).filter(m -> m != null);

    JavaRDD<String> directEmailRecipients =
        parsedData
            .filter(m -> m.getAllRecipients() != null && m.getAllRecipients().length == 1)
            .map(m -> m.getAllRecipients()[0].toString());

    Collection<String> topRecipients =
        directEmailRecipients
            .mapToPair(r -> new Tuple2<>(r, 1))
            .reduceByKey((a, b) -> a + b)
            .top(3, SerializableComparator.serialize((o1, o2) -> o1._2 - o2._2))
            .stream()
            .map(Tuple2::_1)
            .collect(Collectors.toList());

    JavaRDD<String> broadcastEmailSenders =
        parsedData
            .filter(m -> m.getAllRecipients() != null && m.getAllRecipients().length > 1
                && m.getFrom() != null && m.getFrom().length > 0)
            .map(m -> m.getFrom()[0].toString());

    Collection<String> topSender =
        broadcastEmailSenders
            .mapToPair(r -> new Tuple2<>(r, 1))
            .reduceByKey((a, b) -> a + b)
            .top(1, SerializableComparator.serialize((o1, o2) -> o1._2 - o2._2))
            .stream()
            .map(Tuple2::_1)
            .collect(Collectors.toList());

    System.out.println("Top 3 recipients of direct mail: " + String.join(", ", topRecipients));
    System.out.println("Top sender of broadcast mail: " + String.join(", ", topSender));
  }

}
