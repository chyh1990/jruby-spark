package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.jruby.TypeUtils;
import org.jruby.javasupport.Java;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * Created by chenyh on 3/30/16.
 */
public class JFunction2 extends JRubyFunctionBase implements Function2 {
    public JFunction2(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Object call(Object v1, Object v2) throws Exception {
        //Long ret = (Long)callProc2(v1, v2).toJava(Long.class);
        Object ret = callProc2(v1, v2);
        // System.err.println("X " + ret.getClass().getCanonicalName());
        return ret;//JavaUtil.convertRubyToJava(ret);
        //System.err.println("X " + v1.getClass().getName());
       //return (Long)v1 + (Long)v2;
    }
}
