package org.apache.spark.jruby

import java.util.Iterator

import org.apache.spark.util.KnownSizeEstimation
import org.jruby.exceptions.RaiseException
import org.jruby.runtime.builtin.IRubyObject
import org.jruby.{Ruby, RubyEnumerator, RubyStopIteration}

/**
  * Created by chenyh on 4/2/16.
  */
class JRubyIteratableAdaptor[AnyRef](private val runtime: Ruby, private var obj: RubyEnumerator)
  extends java.lang.Iterable[AnyRef] with KnownSizeEstimation {

  private[jruby]
  class JRubyIterator extends java.util.Iterator[AnyRef] {
    private[jruby] var nextObj: IRubyObject = null

    private def tryReadAhead: Boolean = {
      if (nextObj != null) return true
      try {
        nextObj = obj.next(runtime.getCurrentContext)
        true
      } catch {
        case e: RaiseException =>
          if (e.getException.isInstanceOf[RubyStopIteration]) false else throw e
      }
    }

    def hasNext: Boolean = tryReadAhead

    def next: AnyRef = {
      if (!tryReadAhead) throw new NoSuchElementException
      val n: IRubyObject = nextObj
      nextObj = null
      TypeUtils.rubyToJava(runtime, n).asInstanceOf[AnyRef]
    }
  }

  override def iterator: java.util.Iterator[AnyRef] = new JRubyIterator

  override def estimatedSize: Long = 16
}
