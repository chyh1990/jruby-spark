require 'java'
require 'jruby/core_ext'
require 'delegate'

java_import 'org.apache.spark.SparkConf'
java_import 'org.apache.spark.api.java.JavaRDD'
java_import 'org.apache.spark.api.java.JavaSparkContext'
java_import 'scala.Tuple2'
java_import 'org.apache.spark.jruby.ExecutorBootstrap'
java_import 'com.sensetime.utils.ProcToBytesService'

%w(JFunction JFunction2 JVoidFunction JFlatMapFunction JPairFunction).each do |e|
  java_import 'org.apache.spark.jruby.function.' + e
end

ProcToBytesService.new.basicLoad JRuby.runtime

module JRubySpark
  module Patch

    module Tuple
      def empty?
        false
      end

      def [](index)
        if index < 0
          self.java_send("_#{size + index + 1}")
        elsif index >= size
          nil
        else
          self.java_send("_#{index + 1}")
        end
      end

      def to_a
        size.times.each {|i| self[i]}
      end

      def inspect
        "#<#{self.class.to_s} #{self.to_s}>"
      end
    end
  end

end

class Java::Scala::Tuple2
  include JRubySpark::Patch::Tuple
  def size
    2
  end
end

module JRubySpark
  class RDDLike < Delegator
  end

  class RDD < RDDLike
  end

  class PairRDD < RDDLike
  end

  class RDDLike
    def initialize jrdd
      super
    end

    # create delegator method as:
    # def map f = nil, &block
    #   f ||= Proc.new(block) if block_given?
    #   callJava :map, JFunction, f
    # end
    def self.def_transform name, fclazz, ret_clazz = RDD
      define_method(name) do |f = nil, &block|
        f ||= block if block
        ret_clazz.new(callJava(name, fclazz, f))
      end
    end

    def_transform :map, JFunction
    def_transform :flat_map, JFlatMapFunction
    # def_transform :map_partitions, JMapPartitionsFunction
    def_transform :reduce, JFunction2
    def_transform :foreach, JVoidFunction
    def_transform :map_to_pair, JPairFunction, PairRDD

    def __getobj__
      @jrdd
    end

    def __setobj__(obj)
      @jrdd = obj
    end

    protected
    def callJava method, fclazz, f
      raise 'not a lambda' unless Proc === f || Symbol === f
      payload = Marshal.dump(f).to_java_bytes
      @jrdd.__send__(method, fclazz.new(payload))
    end

  end

  class RDD
    def_transform :filter, JFunction
    # def filter &block
    #   f = Proc.new block
    #   wrap = lambda {|x|
    #     f.call(x) ? true.to_java : false.to_java
    #   }
    #   RDD.new(callJava(:filter, JFunction, wrap))
    # end
  end

  class PairRDD
    def_transform :reduce_by_key, JFunction2
  end

  module Functional
    ADD = lambda {|x, y| x + y}
    # SUB = lambda {|x, y| x - y}
  end
end