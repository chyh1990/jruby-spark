require 'java'
require 'jruby/core_ext'
require 'delegate'

java_import 'org.apache.spark.SparkConf'
java_import 'org.apache.spark.api.java.JavaRDD'
java_import 'org.apache.spark.api.java.JavaSparkContext'
java_import 'org.apache.spark.api.java.function.FlatMapFunction'
java_import 'org.apache.spark.api.java.function.Function2'
java_import 'org.apache.spark.api.java.function.PairFunction'
java_import 'scala.Tuple2'
java_import 'org.apache.spark.jruby.ExecutorBootstrap'
java_import 'com.sensetime.utils.ProcToBytesService'
java_import 'org.apache.spark.jruby.function.JFunction'
java_import 'org.apache.spark.jruby.function.JFunction2'
java_import 'org.apache.spark.jruby.function.JVoidFunction'
java_import 'org.apache.spark.jruby.function.JFlatMapFunction'

ProcToBytesService.new.basicLoad JRuby.runtime

module JRubySpark
  class RDD < Delegator
    def initialize jrdd
      super
    end

    def map_partitions f
      callJava :map_partitions, JMapPartitionsFunction, f
    end

    def map f
      callJava :map, JFunction, f
    end

    def reduce f
      callJava :reduce, JFunction2, f
    end


    def foreach f
      callJava :foreach, JVoidFunction, f
    end

    def __getobj__
      @jrdd
    end

    def __setobj__(obj)
      @jrdd = obj
    end

    private
    def callJava method, fclazz, f
      raise 'not a lambda' unless Proc === f && f.lambda?
      payload = Marshal.dump(f).to_java_bytes
      RDD.new(@jrdd.__send__(method, fclazz.new(payload)))
    end
  end

  module Functional
    ADD = lambda {|x, y| x + y}
    # SUB = lambda {|x, y| x - y}
  end
end

module WordCount
  # new FlatMapFunction<String, String>() {
  #     @Override
  #     public Iterable<String> call(String s) throws Exception {
  #         return Arrays.asList(s.split(" "));
  #     }
  # };
  class SplitWord
    include FlatMapFunction
    # def initialize name
    #   @name = name
    # end

    def call s
      s.split
    end
  end

  CONST1 = 'XXX'

  def self.mapper x
    x.split.size
  end

  def self.main
    b = 'aa'
    # a = lambda {|x| x.split.first + 'bb' + CONST1 }
    a = lambda {|x, y = 4|
      p self.class
      p x
      p y
      # return 'early'
      x.split.first + 'bb' + b
    }
    # p Marshal.dump(a)
    # a.call('aa bb')
    # t = Marshal.dump(a)
    # p t

    pb = -> (x) {
      x.each { |e| p e }
      nil
      # x.split.first + b
    }
    t = Marshal.dump(pb)

    #a1 = lambda {|x| a.call('bb') + 'ee'}
    #p a1.to_bytes

    # a2 = lambda {|x|
    #   d = x + b
    #   e = lambda {|y|
    #     y + 1
    #   }
    #   p e.to_bytes
    # }
    # a2.call 'b'

    File.open('proc.bin', 'wb') {|io| io.write t}
    newp = Marshal.load(t)
    p newp
    p newp.call(['dd  bb'])



    puts "START"
    conf = SparkConf.new
    conf.setMaster('local').setAppName('test1')
    # conf.set("spark.closure.serializer", "org.apache.spark.jruby.JRubySerializer")
    ctx = JavaSparkContext.new conf
    # # ctx.broadcast ExecutorBootstrap.new

    rdd = JRubySpark::RDD.new(ctx.textFile(ARGV[0], 2))
    #f = JFunction.new t.to_java_bytes
    # a = lambda {|x| x.split}
    # p a.to_bytes
    # out = rdd.map(f.to_java)
    out = rdd.map(->(x) { WordCount.mapper x })
              .foreach(->(x) { puts x })

    rdd = JRubySpark::RDD.new(ctx.textFile(ARGV[0]))
    wc = rdd.map(->(x) { WordCount.mapper x })
              .reduce(JRubySpark::Functional::ADD)
    puts "WC: #{wc}"

    # p out.collect()
  end
end