require 'test/unit'
require 'jruby_spark/jruby_spark'

require 'generator'

class JRubySparkTest < Test::Unit::TestCase
  include JRubySpark

  class CustomType
    attr_reader :v

    def initialize v
      @v = v
    end

    def to_s
      @v.to_s
    end

    def hash
      @v.hash
    end

    def eql?(other)
      @v.eql? other.v
    end

    def inspect
      "#<CustomType #{@v}>"
    end
  end

  RESOURCE_DIR = File.expand_path(File.dirname(__FILE__))

  def testfile fn
    File.join RESOURCE_DIR, fn
  end

  def setup
    conf = SparkConf.new
    conf.setMaster('local').setAppName('jruby_spark_unittest')
    conf.set('spark.executor.jruby.bootstrap_file', File.expand_path(__FILE__))
    @sc = SparkContext.new conf
  end

  def test_simple
    rdd = @sc.text_file(testfile('words.txt'))
    assert_equal rdd.filter{|x| x.start_with? 'a'}.count, 2

    rdd = @sc.text_file(testfile('sample5.txt'))
    res = rdd.map{|x| x.split.first}.collect.to_a
    expect = ["Jumping", nil, "Presented", nil, "Jumping"]
    assert_equal res, expect
  end

  def test_collect
    size = 1000
    numbers = Generator.numbers(size)

    rdd = @sc.parallelize(numbers)

    assert_equal rdd.take(0), []

    half = size / 2
    assert_equal rdd.take(half), numbers.take(half)

    assert_equal rdd.take(size), numbers

    assert_equal rdd.take(size*2), numbers
  end

  def test_collect_custom_type
    size = 100
    words = Generator.words(size).map{|e| CustomType.new e}

    rdd = @sc.parallelize(words)
    assert_equal rdd.take(0), []
    rdd.take(5).each_with_index {|e, i|
      assert_equal e.v, words[i].v
    }
    rdd.collect.each_with_index {|e, i|
      assert_equal e.v, words[i].v
    }
  end

  def test_collect_as_hash
    words = Generator.words(size)
    rdd = @sc.parallelize(words).map_to_pair{|x| tuple(x, 1)}

    h = rdd.collect_as_map
    assert_equal h, words.map{|e| [e, 1]}.to_h
  end

  def test_filter
    numbers = Generator.numbers(100)
    words = Generator.words(100)

    avg = numbers.inject(:+) / 100.0
    assert_equal @sc.parallelize(numbers).filter{|x| x > avg}.count, numbers.select{|x| x > avg}.count
    assert_equal @sc.parallelize(words).filter{|x| x.size > 5}.count, words.select{|x| x.size > 5}.count
  end

  def test_flat_map
    words = Generator.words(1000)
    rdd = @sc.parallelize(words, 2)
    assert_equal rdd.flat_map{|e| [e, e]}.count, 2000

    # lazy enumerator
  end

  def teardown
    @sc.stop
  end
end

unless JRubySpark.main?
  # don't run the unittest self on executor
  Test::Unit.run = true
end

