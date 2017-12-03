package net.jgp.labs.informix2spark.l520;

import java.math.BigDecimal;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;

public class VectorBuilderBigDecimal implements UDF1<BigDecimal, Vector> {
  private static final long serialVersionUID = -2991355883253063841L;

  @Override
  public Vector call(BigDecimal t1) throws Exception {
    double d = t1.doubleValue();
    return Vectors.dense(d);
  }

}
