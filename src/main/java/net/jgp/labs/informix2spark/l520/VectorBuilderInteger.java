package net.jgp.labs.informix2spark.l520;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;

public class VectorBuilderInteger implements UDF1<Integer, Vector> {
  private static final long serialVersionUID = -2991355883253063841L;

  @Override
  public Vector call(Integer t1) throws Exception {
    double d = t1.doubleValue();
    return Vectors.dense(d);
  }

}
