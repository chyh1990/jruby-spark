package org.apache.spark.jruby

import org.apache.spark.AccumulatorParam

/**
  * Created by chenyh on 4/7/16.
  */
class JLongAccumulatorParam  extends AccumulatorParam[Long]{
  def addInPlace(t1: Long, t2: Long): Long = t1 + t2
  def zero(initialValue: Long): Long = 0L
}
