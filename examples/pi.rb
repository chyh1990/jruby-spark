#!/usr/bin/env ruby

require 'jruby_spark/jruby_spark'

include JRubySpark

def main
  conf = SparkConf.new
  conf.set_app_name 'rubyPi'
  sc = SparkContext.new conf
  partitions = (ARGV[0] || 2).to_i
  n = 100000 * partitions

  count = sc.parallelize(1..n, partitions).map{|x|
    x = rand * 2 - 1
    y = rand * 2 - 1
    x * x + y * y < 1 ? 1 : 0
  }.reduce(:+)

  puts "Pi is roughly #{4.0 * count / n}"

  sc.stop
end

main if JRubySpark.main?
