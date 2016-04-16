package org.apache.spark.jruby;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyProcess;
import org.jruby.internal.runtime.GlobalVariable;
import org.jruby.internal.runtime.ValueAccessor;
import org.jruby.ir.operands.Boolean;
import org.jruby.main.DripMain;
import org.jruby.runtime.IAccessor;

import java.io.Serializable;

/**
 * Created by chenyh on 3/25/16.
 */
public class ExecutorBootstrap {
    private static ExecutorBootstrap instance = null;
    public static final String SPARK_BOOTSTRAP = "classpath:spark_main.rb";
    Ruby runtime;
    String replHTTP = null;

    private ExecutorBootstrap() {
        SparkEnv env = SparkEnv.get();
        String bootstrapFile = SPARK_BOOTSTRAP;
        if (env != null) {
            bootstrapFile = env.conf().get("spark.executor.jruby.bootstrap_file", SPARK_BOOTSTRAP);
            replHTTP = env.conf().get("spark.executor.jruby.repl_http", null);
        }

        System.err.println("Initializing new ruby with " + bootstrapFile);
        RubyInstanceConfig config = new RubyInstanceConfig();
        String args[] = {bootstrapFile};
        config.processArguments(args);

        runtime = Ruby.newInstance(config);

        IAccessor d = new ValueAccessor(runtime.newString("executor"));
        runtime.getGlobalVariables().defineReadonly("$JRUBY_SPARK_PROCESS", d, GlobalVariable.Scope.GLOBAL);
        // FIXME conflict with scala loader?
        Thread.currentThread().setContextClassLoader(runtime.getJRubyClassLoader());

        // FIXME realpath problem
        runtime.runFromMain(config.getScriptSource(), "");
    }

    public static ExecutorBootstrap getInstance() {
        if (instance == null) {
            synchronized (ExecutorBootstrap.class) {
                if (instance == null) {
                    instance = new ExecutorBootstrap();
                }
            }
        }
        return instance;
    }

    public Ruby getRuntime() {
        return runtime;
    }

    public boolean isREPL() { return replHTTP != null; }

    public String getReplHTTP() { return replHTTP; }
}
