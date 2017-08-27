package net.jgp.labs.informix2spark.tools;

import java.util.HashSet;
import java.util.Set;

import scala.collection.JavaConversions;

public class ScalaSet {

  /**
   * Adapted from
   * https://stackoverflow.com/questions/3025291/example-of-using-scala-collection-immutable-set-from-java.
   * 
   * @param args
   */
  public static void main(String[] args) {
    Set<String> javaSet = new HashSet<>();
    javaSet.add("one");
    javaSet.add("two");
    javaSet.add("three");

    System.out.println(javaSet); // prints [one, two, three]

    scala.collection.Set<String> s = JavaConversions
        .asScalaSet(javaSet);
    System.out.println(s); // prints Set(one, two, three)
  }

}
