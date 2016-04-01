package org.apache.spark.jruby;

import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.jruby.Ruby;

import java.io.Serializable;

/**
 * Created by chenyh on 3/31/16.
 */
public class JRubyDummySerializer extends JavaSerializer {
    @Override
    public SerializerInstance newInstance() {
        // do extra initialization
        // Ruby.setThreadLocalRuntime(Ruby.getGlobalRuntime());
        return super.newInstance();
    }
}
