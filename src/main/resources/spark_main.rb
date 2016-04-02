require '/data/chenyh/work/spark/test_spark1/src/main/resources/jruby_spark.rb'

module WordCount
  # new FlatMapFunction<String, String>() {
  #     @Override
  #     public Iterable<String> call(String s) throws Exception {
  #         return Arrays.asList(s.split(" "));
  #     }
  # };


  CONST1 = 'XXX'

  def self.mapper x
    x.split.size
  end

  class NotIter
    def initialize name
      @name = name
    end

    def get
      @name
    end
  end

  class MyType
    attr_reader :name
    def initialize name
      @name = name
    end

    def get
      @name
    end

    def to_s
      "ME:#{@name}"
    end

    def eql?(other)
      self.name.eql? other.name
    end

    def hash
      self.name.hash
    end
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

    p = Proc.new {|x|
      #x.each {|y| puts y}
      puts x
    }
    t = Marshal.dump(p)
    p t
    p1 = Marshal.load(t)
    p p1
    p1.call([1,2])
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
    #conf.set("spark.closure.serializer", "org.apache.spark.jruby.JRubyDummySerializer")
    ctx = JavaSparkContext.new conf
    # # ctx.broadcast ExecutorBootstrap.new

    rdd = JRubySpark::RDD.new(ctx.textFile(ARGV[0]))
    #f = JFunction.new t.to_java_bytes
    # a = lambda {|x| x.split}
    # p a.to_bytes
    # out = rdd.map(f.to_java)
    #out = rdd.map(->(x) { WordCount.mapper x })
    #          .foreach(->(x) { puts x })

    rdd = JRubySpark::RDD.new(ctx.textFile(ARGV[0]))
    puts "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
    #wc = rdd.map{|x| x.split.size }.map(->(x) {x}).reduce(:+)
    #          #.reduce(->(x,y){x+y})
    #puts "WC: #{wc}"

    # p rdd.map{|x| [x.split.size, 0]}.foreach{|x| p x}
    # p out.collect()
    delim = /[^\w']+/
    rdd.flat_map{|x| x.split delim }
        .filter{|x| x.start_with? 'a' }
        .map_to_pair{|x| [x, 1]}
        .reduce_by_key(:+)
        .foreach{|x| puts x}

    rdd = JRubySpark::RDD.new(ctx.textFile(ARGV[0]))
    #rdd.map_partitions_with_index{|x, y|
    #  puts x
    #  y.each {|z| puts z}
    #  y
    #}.foreach{|x| puts x }

    #p rdd.flat_map{|x| x.split delim }.map{|x| MyType.new x }
    #    .map_to_pair{|x| [x, 1]}
    #    .reduce_by_key(:+).saveAsTextFile("outdir")
        #.foreach{|x| puts x }
    # sleep
  end
end