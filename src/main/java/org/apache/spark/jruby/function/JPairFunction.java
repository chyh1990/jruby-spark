package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by chenyh on 3/31/16.
 */
public class JPairFunction extends JRubyFunctionBase implements PairFunction {
    public JPairFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Tuple2 call(Object o) throws Exception {
        Object ret = callProc1(o);
        if (ret instanceof Tuple2)
            return (Tuple2)ret;
        else
            throw new RuntimeException("PairFunction must return Tuple2");
    }
}
