package org.apache.spark.jruby;

import org.jruby.*;
import org.jruby.javasupport.JavaObject;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.builtin.IRubyObject;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by chenyh on 3/31/16.
 */
public class TypeUtils {

    public static Iterable rubyToIterable(Ruby runtime, Object rbObj) {
        if (rbObj == null)
            throw runtime.newArgumentError("Iterator must not be null");
        if (rbObj instanceof JRubyIteratableAdaptor)
            return (Iterable)rbObj;
        if (rbObj instanceof RubyObject) {
            RubyObject obj = (RubyObject) rbObj;
            if (obj instanceof RubyEnumerator) {
                return new JRubyIteratableAdaptor(runtime, (RubyEnumerator) rbObj);
                // FIXME opt for array
            } else if (obj.respondsTo("each")) {
                RubyEnumerator enumerator = (RubyEnumerator) RubyEnumerator.enumeratorize(runtime, obj, "each");
                return new JRubyIteratableAdaptor(runtime, enumerator);
            } else {
                throw runtime.newArgumentError("not a Enumerator or fail to convert to enumerator: " + rbObj.getClass().getName());
            }
        } else if (rbObj instanceof Iterable) {
            return (Iterable)rbObj;
        } else {
            throw new RuntimeException("Unsupported type to Iterable");
        }
    }

    // FIXME: improve this
    public static Tuple2 rubyToTuple2(Ruby runtime, IRubyObject obj) {
        if (obj instanceof Tuple2) {
            return (Tuple2)obj;
        } else if (obj instanceof RubyArray) {
            IRubyObject[] rets = ((RubyArray) obj).toJavaArray();
            if (rets.length != 2)
                throw new RuntimeException("Expect Tuple2");
            return new Tuple2(rubyToJava(runtime, rets[0]), rubyToJava(runtime, rets[1]));
        } else {
            throw new RuntimeException("not a tuple");
        }
    }

    public static IRubyObject javaToRuby(Ruby runtime, Object obj) {
        if (obj instanceof RubyObjectWrapper) {
            RubyObjectWrapper wrapper = (RubyObjectWrapper) obj;
            return wrapper.get();
        } else if (obj instanceof Tuple2) {
            Tuple2 t = (Tuple2)obj;
            return JavaUtil.convertJavaToRuby(runtime,
                    new Tuple2(javaToRuby(runtime, t._1), javaToRuby(runtime, t._2)));
        } else {
            return JavaUtil.convertJavaToRuby(runtime, obj);
        }
    }

    public static Object rubyToJava(Ruby runtime, IRubyObject rubyObject) {
        // array!
        if (rubyObject == null || rubyObject.isNil())
            return null;

        if (rubyObject instanceof RubyBoolean)
            return rubyObject.isTrue();
        else if (rubyObject instanceof RubyFixnum)
            return ((RubyFixnum) rubyObject).getLongValue();
        else if (rubyObject instanceof RubyFloat)
            return ((RubyFloat) rubyObject).getValue();
        else if (rubyObject instanceof RubyBignum)
            return ((RubyBignum) rubyObject).getBigIntegerValue();

        IRubyObject origObject = rubyObject;
        if (rubyObject.dataGetStruct() instanceof JavaObject) {
            rubyObject = (IRubyObject) rubyObject.dataGetStruct();
            if ( rubyObject == null ) {
                throw new RuntimeException("dataGetStruct returned null for " + origObject.getType().getName());
            }
        } /* else if (rubyObject.respondsTo("java_object")) {
            rubyObject = rubyObject.callMethod(runtime.getCurrentContext(), "java_object");
            if( rubyObject == null ) {
                throw new RuntimeException("java_object returned null for " + origObject.getType().getName());
            }
        } */

        if (rubyObject instanceof JavaObject) {
            return ((JavaObject) rubyObject).getValue();
        }

        // FIXME: should we try our best to convert types to java types?
        // String & Array?
        return new RubyObjectWrapper(rubyObject);
    }
}
