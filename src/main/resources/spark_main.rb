require 'jruby_spark/jruby_spark'

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
    conf = JRubySpark::SparkConf.new
    conf.setMaster('local').setAppName('test1')
    #conf.set("spark.closure.serializer", "org.apache.spark.jruby.JRubyDummySerializer")
    ctx = JRubySpark::SparkContext.new conf
    # # ctx.broadcast ExecutorBootstrap.new

    #rdd = JRubySpark::RDD.new(ctx.textFile(ARGV[0]))
    #f = JFunction.new t.to_java_bytes
    # a = lambda {|x| x.split}
    # p a.to_bytes
    # out = rdd.map(f.to_java)
    #out = rdd.map(->(x) { WordCount.mapper x })
    #          .foreach(->(x) { puts x })

    rdd = ctx.text_file(ARGV[0])
    puts "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
    #wc = rdd.map{|x| x.split.size }.map(->(x) {x}).reduce(:+)
    #          #.reduce(->(x,y){x+y})
    #puts "WC: #{wc}"

    # p rdd.map{|x| [x.split.size, 0]}.foreach{|x| p x}
    # p out.collect()
    delim = /[^\w']+/
    rdd.flat_map{|x| x.split delim }
        .filter{|x| x.start_with? 'a' }
        .map_to_pair{|x| tuple(x, 1) }
        .reduce_by_key(:+)
        .foreach{|x| puts x}

    rdd = ctx.text_file(ARGV[0], 2)
    puts rdd.map_partitions {|x|
      Enumerator.new do |y|
        x.each {|l|
          y << l if l.start_with? 'Jump'
        }
      end
    }.collect.to_a

    #pi
    num_samples = 50000
    count = JRubySpark::RDD.new(ctx.parallelize((1..num_samples).to_a)).map{|_|
        x = rand
        y = rand
        x*x + y*y < 1 ? 1 : 0
    }.reduce(:+)
    puts "Pi is roughly #{4.0 * count / num_samples}"

    # puts rdd.collect.to_a
    #rdd.map_partitions_with_index{|x, y|
    #  puts x
    #  y.each {|z| puts z}
    #  y
    #}.foreach{|x| puts x }

    p rdd.flat_map{|x| x.split delim }.map{|x| MyType.new x }
        .map_to_pair{|x| tuple(x, 1)}
        .reduce_by_key(:+) #.saveAsTextFile("outdir")
        .foreach{|x| puts x }
    # sleep
  end

  def self.main2
    clv1 = 'xx'
    a = lambda {|x|
      p clv1
      ['a','b'].each do |y|
        p y + clv1
      end
    }
    t = Marshal.dump(a)
    p t
    a1 = Marshal.load(t)
    a1.call('c')
  end
end

if JRubySpark.main?
  WordCount.main
end