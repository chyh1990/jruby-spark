package org.apache.spark.jruby;

import org.apache.log4j.Logger;
import org.apache.spark.SparkEnv;
import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;
import org.jruby.RubyProcess;
import org.jruby.main.DripMain;

import java.io.Serializable;

/**
 * Created by chenyh on 3/25/16.
 */
public class ExecutorBootstrap {
    private static ExecutorBootstrap instance = null;
    public static final String SPARK_BOOTSTRAP = "classpath:spark_main.rb";
    Ruby runtime;
    private ExecutorBootstrap() {
        System.err.println("Initializing new ruby env");
        RubyInstanceConfig config = new RubyInstanceConfig();
        String args [] = new String[1];
        args[0] = SPARK_BOOTSTRAP;
        config.processArguments(args);

        runtime = Ruby.newInstance(new RubyInstanceConfig());
        // FIXME conflict with scala loader?
        Thread.currentThread().setContextClassLoader(runtime.getJRubyClassLoader());
        // FIXME realpath problem
        runtime.runFromMain(config.getScriptSource(), "");
    }

    public static synchronized ExecutorBootstrap getInstance() {
        if (instance == null){
            instance = new ExecutorBootstrap();
        }
        return instance;
    }

    public Ruby getRuntime() {
        return runtime;
    }
}
