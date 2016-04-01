package org.apache.spark.jruby

import java.io._

import org.jruby.runtime.builtin.IRubyObject
import org.apache.spark.util.{KnownSizeEstimation, Utils}
import org.jruby.RubyMarshal
import org.jruby.runtime.Block
import org.jruby.runtime.marshal.{MarshalStream, UnmarshalStream}
/**
  * Created by chenyh on 4/1/16.
  */
class RubyObjectWrapper(private var _obj: IRubyObject)
  extends KnownSizeEstimation with Externalizable {

  def this() = this(null)

  override def estimatedSize: Long = {
    //println("HERE")
    10
  }

  override def readExternal(objectInput: ObjectInput): Unit = {
    val runtime = ExecutorBootstrap.getInstance().getRuntime()
    val size = objectInput.readInt()
    val buf = Array.ofDim[Byte](size)
    objectInput.readFully(buf)
    val rawInput = new ByteArrayInputStream(buf)
    _obj = new UnmarshalStream(runtime, rawInput, null, false).unmarshalObject
  }

  override def writeExternal(objectOutput: ObjectOutput): Unit = {
    val runtime = ExecutorBootstrap.getInstance().getRuntime()
    //RubyMarshal.dump(runtime.getMarshal(), Array(obj), Block.NULL_BLOCK).
    //objectOutput.wr
    val stringOutput = new ByteArrayOutputStream
    val output = new MarshalStream(runtime, stringOutput, -1)
    output.dumpObject(_obj)
    objectOutput.writeInt(stringOutput.size())
    objectOutput.write(stringOutput.toByteArray)
  }

  def get() = _obj

  override def toString() : String = "#<Wrap: " + _obj.asJavaString() + ">"
  override def equals(o: Any) : Boolean = o match {
    case that: RubyObjectWrapper => _obj.eql(that._obj)
    case that: IRubyObject => _obj.eql(that)
    case _ => false
  }
  override def hashCode(): Int = _obj.hashCode()
}
