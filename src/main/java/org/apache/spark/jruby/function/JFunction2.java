package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by chenyh on 3/30/16.
 */
public class JFunction2 extends JRubyFunctionBase implements Function2 {
    public JFunction2(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Object call(Object v1, Object v2) throws Exception {
        return callProc2(v1, v2);
    }
}
