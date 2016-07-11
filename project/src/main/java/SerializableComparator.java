import java.io.Serializable;
import java.util.Comparator;

/**
 * Taken from: http://stackoverflow.com/questions/19433135/notserializableexception-when-sorting-in-spark
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {

  static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
    return comparator;
  }

}