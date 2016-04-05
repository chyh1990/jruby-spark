package org.apache.spark.jruby;

import org.jruby.Main;

/**
 * Created by chenyh on 4/5/16.
 */
public class ScriptMain {
    public static void main(String []args) {
        //String[] newArgs = new String[args.length + 1];
        //newArgs[0] = "-Djruby.ir.writing=true";
        //System.arraycopy(args, 0, newArgs, 1, args.length);
        Main.main(args);
    }
}
