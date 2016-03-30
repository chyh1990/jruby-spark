package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.VoidFunction;
/**
 * Created by chenyh on 3/30/16.
 */
public class JVoidFunction extends JRubyFunctionBase implements VoidFunction {
    public JVoidFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public void call(Object o) throws Exception {
        callProc1NoResult(o);
    }
}
