package org.apache.spark.jruby.function;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * Created by chenyh on 3/30/16.
 */
public class JFlatMapFunction extends JRubyFunctionBase implements FlatMapFunction {
    public JFlatMapFunction(byte[] bytecode) {
        super(bytecode);
    }

    @Override
    public Iterable call(Object o) throws Exception {
        System.out.println("FLATMAP " + o);
        callProc1(o);

        return null;
    }
}
