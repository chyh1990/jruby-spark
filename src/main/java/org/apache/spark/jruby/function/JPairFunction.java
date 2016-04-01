package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.PairFunction;
import org.jruby.RubyProcess;
import org.jruby.runtime.builtin.IRubyObject;
import scala.Tuple2;

import static org.apache.spark.jruby.TypeUtils.rubyToTuple2;

/**
 * Created by chenyh on 3/31/16.
 */
public class JPairFunction extends JRubyFunctionBase implements PairFunction {
    public JPairFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Tuple2 call(Object o) throws Exception {
        return rubyToTuple2(getRuntime(), callProc1NoConvert(o));
    }
}
