#!/usr/bin/env ruby

require 'jruby_spark/jruby_spark'

include JRubySpark

def main
  raise 'no input file' unless ARGV[0]
  conf = SparkConf.new
  conf.set_app_name 'wordcount'
  sc = SparkContext.new conf

  lines = sc.text_file ARGV[0]
  counts = lines.flat_map{|x| x.downcase.split(/[^\w']+/)}
    .map_to_pair{|x| tuple(x, 1)}.reduce_by_key(:+).collect_as_map

  counts.each do |k, v|
    puts "#{k}: #{v}"
  end
end

main if JRubySpark.main?
