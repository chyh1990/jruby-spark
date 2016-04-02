require 'java'
require 'jruby/core_ext'
require 'delegate'

java_import 'org.apache.spark.SparkConf'
java_import 'org.apache.spark.api.java.JavaRDD'
java_import 'org.apache.spark.api.java.JavaDoubleRDD'
java_import 'org.apache.spark.api.java.JavaSparkContext'
java_import 'scala.Tuple2'
java_import 'org.apache.spark.jruby.ExecutorBootstrap'
java_import 'com.sensetime.utils.ProcToBytesService'

%w(JFunction JFunction2 JVoidFunction JFlatMapFunction JPairFunction JDoubleFunction).each do |e|
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

  class DoubleRDD < RDDLike
  end

  class RDDLike
    java_import org.apache.spark.jruby.TypeUtils

    def self.to_java_iter it
      TypeUtils::rubyToIterable JRuby.runtime, it
    end

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
        if ret_clazz
          ret_clazz.new(callJava(name, fclazz, f))
        else
          callJava(name, fclazz, f)
        end
      end
    end

    def aggregate zero, seqOp, combOp
      @jrdd.__send__(:aggregate, zero, create_func!(JFunction2, seqOp), create_func!(JFunction2, combOp))
    end

    def cartesian pair_rdd
      raise 'not a pair rdd' unless PairRDD === pair_rdd
      PairRdd.new(@jrdd.__send__(:cartesian, pair_rdd.__getobj__))
    end


    def_transform :flat_map, JFlatMapFunction
    def_transform :flat_map_to_double, JFlatMapFunction, DoubleRDD
    # def_transform :map_partitions, JMapPartitionsFunction
    def_transform :reduce, JFunction2
    def_transform :foreach, JVoidFunction
    def_transform :map, JFunction
    def_transform :map_to_double, JDoubleFunction, DoubleRDD
    def_transform :map_to_pair, JPairFunction, PairRDD

    def map_partitions_with_index f = nil, preservesPartitioning = false, &block
      f ||= block if block
      to_iter_f2 = lambda {|v1, v2| RDDLike.to_java_iter f.call(v1, v2) }
      RDD.new @jrdd.mapPartitionsWithIndex(create_func!(JFunction2, to_iter_f2), preservesPartitioning)
    end

    def map_partitions f = nil, preservesPartitioning = false, &block
      f ||= block if block
      to_iter_f2 = lambda {|v1, v2| RDDLike.to_java_iter f.call(v1, v2) }
      RDD.new @jrdd.mapPartitions(create_func!(JFlatMapFunction, f), preservesPartitioning)
    end


    def __getobj__
      @jrdd
    end

    def __setobj__(obj)
      @jrdd = obj
    end

    protected
    def callJava method, fclazz, f, args = []
      args = [create_func!(fclazz, f)] + args
      @jrdd.__send__(method, *args)
    end

    def create_func! fclazz, f
      raise 'not a lambda' unless Proc === f || Symbol === f
      payload = Marshal.dump(f).to_java_bytes
      fclazz.new(payload)
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
    def to_double_rdd
      DoubleRDD.from_rdd self
    end
  end

  class DoubleRDD
    def self.from_rdd rdd
      raise 'not an rdd' unless RDD === rdd
      @jrdd = JavaDoubleRDD.fromRDD rdd.__getobj__.rdd
    end
  end

  class PairRDD
    def_transform :reduce_by_key, JFunction2
  end

  module Functional
    ADD = lambda {|x, y| x + y}
    # SUB = lambda {|x, y| x - y}
  end
end