package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import static org.apache.spark.jruby.TypeUtils.rubyToIterable;

/**
 * Created by chenyh on 4/8/16.
 */
public class JPairFlatMapFunction
        extends JRubyFunctionBase implements PairFlatMapFunction {
    public JPairFlatMapFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Iterable<Tuple2> call(Object o) throws Exception {
        Iterable it = rubyToIterable(getRuntime(), callProc1NoConvert(o));
        return it;
    }
}
