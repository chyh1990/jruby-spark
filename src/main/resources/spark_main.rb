require 'java'
require 'jruby/core_ext'

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
java_import 'org.apache.spark.jruby.function.JFlatMapFunction'

ProcToBytesService.new.basicLoad JRuby.runtime

class Proc

  def self._load args
    Proc._from_bytes args[0], args[1]
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
      # nil
      ''
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
    p newp.call('dd  bb')



    puts "START"
    conf = SparkConf.new
    conf.setMaster('local').setAppName('test1')
    # conf.set("spark.closure.serializer", "org.apache.spark.jruby.JRubySerializer")
    ctx = JavaSparkContext.new conf
    # # ctx.broadcast ExecutorBootstrap.new

    rdd = ctx.textFile ARGV[0]
    f = JFunction.new t.to_java_bytes
    # a = lambda {|x| x.split}
    # p a.to_bytes
    # out = rdd.map(f.to_java)
    out = rdd.map_partitions(JFlatMapFunction.new(t.to_java_bytes))
    p out.collect()
  end
end