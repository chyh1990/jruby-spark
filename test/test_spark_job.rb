require 'test/unit'
require 'jruby_spark/jruby_spark'

class JRubySparkTest < Test::Unit::TestCase
  include JRubySpark

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

  def teardown
    @sc.stop
  end
end

unless JRubySpark.main?
  # don't run the unittest self on executor
  Test::Unit.run = true
end

