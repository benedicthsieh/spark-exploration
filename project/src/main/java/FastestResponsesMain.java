import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by benedicthsieh on 7/11/16.
 */
public class FastestResponsesMain {

  private static class MessageWithResponse implements Serializable {
    final MessageFields initial;
    final MessageFields response;
    final Duration responseTime;

    public MessageWithResponse(MessageFields initial, MessageFields response) {
      this.initial = initial;
      this.response = response;
      this.responseTime = Duration.between(initial.sentDate.toInstant(), response.sentDate.toInstant()).abs();
    }

    public String prettyPrint() {
      return String.format("Original: %s\n Response: %s\n Response Time:%s seconds",
          initial.prettyPrint(),
          response.filepath,
          responseTime.getSeconds());
    }
  }

  private static class ResponseCmp implements Serializable, Comparator<MessageWithResponse> {
    @Override
    public int compare(MessageWithResponse o1, MessageWithResponse o2) {
      return o1.responseTime.compareTo(o2.responseTime);
    }
  }

  private static class MessageFields implements Serializable {
    final Date sentDate;
    final String filepath;
    final String subject;
    final String sender;
    final String recipient;

    MessageFields(Date sentDate, String filepath, String subject, String sender, String recipient) {
      this.sentDate = sentDate;
      this.filepath = filepath;
      this.subject = subject;
      this.sender = sender;
      this.recipient = recipient;
    }

    String prettyPrint() {
      return String.format("  Path: %s\n" +
          "  Subject: %s\n" +
          "  Sender: %s\n" +
          "  Recipient: %s\n", filepath, subject, sender, recipient);
    }
  }

  public static void main(String[] args) {
    String logPath = Utils.emailsPath;
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<String, String> logData = sc.wholeTextFiles(logPath);

    JavaPairRDD<Utils.UnorderedUserPair, Iterable<MessageFields>> emailsGroupedByParticipants = logData
        .flatMapToPair(p -> readRelevantFields(p._1, p._2))
        .groupByKey();

    Collection<MessageWithResponse> withResponses = emailsGroupedByParticipants
        .values()
        .flatMap(FastestResponsesMain::groupIntoResponses)
        .takeOrdered(5, new ResponseCmp());

    System.out.println("5 fastest response times:");
    withResponses.forEach(m -> System.out.println(m.prettyPrint()));
  }

  // Emit a tuple for each sender-recipient pair in the email.
  // Key is the sender,recipient (unordered).
  // Value is some fields from the email we want to extract.
  private static Collection<Tuple2<Utils.UnorderedUserPair,MessageFields>> readRelevantFields(String path, String content) {
    MimeMessage contentMsg = Utils.stringToMimeMessage(content);
    try {
      if (contentMsg == null || contentMsg.getFrom() == null || contentMsg.getFrom().length < 1
          || contentMsg.getAllRecipients() == null || contentMsg.getAllRecipients().length < 1) {
        return Collections.emptyList();
      }

      Date sentDate = contentMsg.getSentDate();
      String subject = contentMsg.getSubject();
      String sender = contentMsg.getFrom()[0].toString();

      return Arrays.stream(contentMsg.getAllRecipients())
          .distinct()  // turns out you can have dupes across to, cc, bcc lines.
          .map(r -> new Tuple2<>(
              new Utils.UnorderedUserPair(sender, r.toString()),
              new MessageFields(
                sentDate,
                path,
                subject,
                sender,
                r.toString())))
          .collect(Collectors.toList());
    } catch (MessagingException e) {
      return Collections.emptyList();
    }
  }


  private static class MessageCmp implements Serializable, Comparator<MessageFields> {
    @Override
    public int compare(MessageFields o1, MessageFields o2) {
      int cmp = o1.subject.length() - o2.subject.length();
      return cmp != 0 ? cmp : o1.sentDate.compareTo(o2.sentDate);
    }
  }

  private static Iterable<MessageWithResponse> groupIntoResponses(Iterable<MessageFields> messages) {
    // First, sort them from smallest subject length to largest and then from earliest to latest.
    // This means that replies will always be later in the list.
    List<MessageFields> sortedMessages =
        StreamSupport.stream(messages.spliterator(), false)
            .sorted(new MessageCmp())
            .collect(Collectors.toList());

    Collection<MessageWithResponse> withResponses = new ArrayList<>();

    for (int i = 0; i < sortedMessages.size(); i++) {
      MessageFields initial = sortedMessages.get(i);
      for (int j = i + 1; j < sortedMessages.size(); j++) {
        MessageFields candidate = sortedMessages.get(j);
        if (isResponse(initial, candidate)) {
          withResponses.add(new MessageWithResponse(initial, candidate));
        }
      }
    }
    return withResponses;
  }

  // Note that our definition of response is a little idiosyncratic. Responses could have negative response time,
  // and there can be an exponential number of response pairs in a chain. Eg:
  // 1. A -> B -- Hello
  // 2. B -> A -- Re: Hello
  // 3. A -> B -- Re:Re:Hello
  // 4. B -> A -- Re:Re:Re:Hello
  //
  // 4 is a response to 3 as expected. However, both 4 AND 2 are responses to 1 according to the definition
  // we've been given.
  private static boolean isResponse(MessageFields initial, MessageFields response) {
    return initial.recipient.equals(response.sender)
        && initial.sender.equals(response.recipient)
        && response.subject.contains(initial.subject);
  }
}