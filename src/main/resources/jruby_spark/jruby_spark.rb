require 'java'
require 'jruby/core_ext'
require 'delegate'

java_import 'com.sensetime.utils.ProcToBytesService'
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

class Java::Scala::Tuple3
  include JRubySpark::Patch::Tuple
  def size
    3
  end
end

module JRubySpark
  java_import 'org.apache.spark.SparkConf'
  java_import 'org.apache.spark.api.java.JavaRDD'
  java_import 'org.apache.spark.SparkFiles'
  java_import 'org.apache.spark.storage.StorageLevel'
  java_import 'org.apache.spark.api.java.JavaDoubleRDD'
  java_import 'org.apache.spark.api.java.JavaSparkContext'

  %w(JFunction JFunction2 JVoidFunction JFlatMapFunction JPairFunction JDoubleFunction JPairFlatMapFunction).each do |e|
    java_import 'org.apache.spark.jruby.function.' + e
  end
  java_import 'org.apache.spark.jruby.RubyObjectWrapper'

  Tuple2 = Java::Scala::Tuple2

  class RDDError < StandardError
    def initialize(msg = 'unknown rdd error')
      super
    end
  end

  def self.executor?
    $JRUBY_SPARK_PROCESS == 'executor'
  end

  def self.main?
    !self.executor?
  end

  module Helpers
    java_import org.apache.spark.jruby.TypeUtils
    def self.to_iter it
      TypeUtils::rubyToIterable JRuby.runtime, it
    end
  end

  module Functional
    ADD = lambda {|x, y| x + y}
    # SUB = lambda {|x, y| x - y}
  end

  def self.add_vendor_gems!
    vendor_path = 'uri:classloader:/vendor/bundle'
    gem_path = File.join(vendor_path, 'jruby', RUBY_VERSION)
    Gem::Specification.add_dir gem_path
  end
end

require 'jruby_spark/rdd'
require 'jruby_spark/streaming'
require 'jruby_spark/sql'

module Kernel
  def tuple *args
    case args.length
      when 2
        Java::Scala::Tuple2.new(*args)
      when 3
        Java::Scala::Tuple3.new(*args)
      else
        raise ArgumentError('invalid tuple size')
    end
  end

  def to_iter
    JRubySpark::Helpers.to_iter self
  end
end
