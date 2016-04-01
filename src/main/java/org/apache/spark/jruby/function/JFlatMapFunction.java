package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.jruby.runtime.Block;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Iterator;

import static org.apache.spark.jruby.TypeUtils.rubyToIterable;

/**
 * Created by chenyh on 3/30/16.
 */
public class JFlatMapFunction extends JRubyFunctionBase implements FlatMapFunction {
    public JFlatMapFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Iterable call(Object o) throws Exception {
        // FIXME
        Iterable it = rubyToIterable(getRuntime(), callProc1NoConvert(o));
        return it;
    }
}
