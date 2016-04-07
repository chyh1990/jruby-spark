#!/usr/bin/env ruby

require 'jruby_spark/jruby_spark'

include JRubySpark

def main
  if ARGV.size != 1
    $stderr.puts "Usage: sort <file>"
    exit 1
  end

  sc = SparkContext.new app_name: 'RubySort'

  lines = sc.text_file ARGV[0]
  sorted_count = lines.flat_map{|x| x.split}.map_to_pair{|x|
    tuple(x.to_i, 1)}.sort_by_key{|x| x}
  # This is just a demo on how to bring all the sorted data back to a single node.
  # In reality, we wouldn't want to collect all the data to the driver node.
  output = sorted_count.collect
  output.each do |e|
    puts e[0]
  end

  sc.stop
end

main if JRubySpark.main?
