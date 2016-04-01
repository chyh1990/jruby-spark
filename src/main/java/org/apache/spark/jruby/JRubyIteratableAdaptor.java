package org.apache.spark.jruby;

import org.jruby.Ruby;
import org.jruby.RubyEnumerator;
import org.jruby.RubyStopIteration;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.apache.spark.jruby.TypeUtils.rubyToJava;

/**
 * Created by chenyh on 3/31/16.
 */
public
class JRubyIteratableAdaptor implements Iterable {
    private RubyEnumerator obj;
    private Ruby runtime;
    class JRubyIterator implements Iterator {
        IRubyObject nextObj = null;

        private boolean tryReadAhead() {
            if (nextObj != null)
                return true;
            try {
                nextObj = obj.next(runtime.getCurrentContext());
                return true;
            } catch (RaiseException e) {
                if(e.getException() instanceof RubyStopIteration) {
                    return false;
                } else {
                    throw e;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return tryReadAhead();
        }

        @Override
        public Object next() {
            if(!tryReadAhead())
                throw new NoSuchElementException();
            IRubyObject n = nextObj;
            nextObj = null;
            return rubyToJava(runtime, n);
        }
    }

    @Override
    public Iterator iterator() {
        return new JRubyIterator();
    }

    public JRubyIteratableAdaptor(Ruby runtime, RubyEnumerator obj) {
        this.obj = obj;
        this.runtime = runtime;
    }
}
