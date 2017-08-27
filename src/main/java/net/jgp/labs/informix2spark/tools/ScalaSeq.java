/**
 * 
 */
package net.jgp.labs.informix2spark.tools;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConversions;

/**
 * @author jgp
 *
 */
public class ScalaSeq {

  /**
   * @param args
   */
  public static void main(String[] args) {
    List<String> javaList = new ArrayList<>();
    javaList.add("one");
    javaList.add("two");
    javaList.add("three");

    System.out.println(javaList); // prints [one, two, three]

    scala.collection.Seq<String> s = JavaConversions
        .asScalaBuffer(javaList);
    System.out.println(s); // prints Buffer(one, two, three)
  }

}
