package org.apache.spark.jruby.function;

/**
 * Created by chenyh on 3/30/16.
 */
public class JFunction extends JRubyFunctionBase implements org.apache.spark.api.java.function.Function {

    public JFunction(byte[] bytecode) {
        super(bytecode);
        // System.err.println("CONSTRUCTED " + bytecode.length);
    }

    @Override
    public Object call(Object v1) throws Exception {
        // System.err.println("HERE " + v1);

        // System.err.println("RET " + ret);
        return callProc1(v1);
    }
}
