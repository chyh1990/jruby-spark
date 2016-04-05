package org.apache.spark.jruby;

import org.jruby.Main;

/**
 * Created by chenyh on 3/31/16.
 */
public class REPLMain {
    public static final String REPL_BOOTSTRAP = "classpath:jruby_spark/repl.rb";
    public static void main(String []args) {
        String[] newArgs = new String[args.length + 1];
        newArgs[0] = REPL_BOOTSTRAP;
        System.arraycopy(args, 0, newArgs, 1, args.length);
        Main.main(newArgs);
    }
}
