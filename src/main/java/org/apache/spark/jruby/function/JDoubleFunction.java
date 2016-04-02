package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.DoubleFunction;

/**
 * Created by chenyh on 4/2/16.
 */
public class JDoubleFunction extends JRubyFunctionBase implements DoubleFunction {
    public JDoubleFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public double call(Object o) throws Exception {
        return callProc1NoConvert(o).convertToFloat().getDoubleValue();
    }
}
