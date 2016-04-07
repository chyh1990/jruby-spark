#!/usr/bin/env ruby

require 'set'
require 'jruby_spark/jruby_spark'
include JRubySpark

NUM_EDGES=200
NUM_VERTICES=100
RAND_NUM=rand(42)

def generate_graph
  edges = Set.new
  while edges.size < NUM_EDGES
    src = rand(NUM_EDGES)
    dst = rand(NUM_EDGES)
    edges.add tuple(src, dst) if src != dst
  end
  edges
end

def main
  sc = SparkContext.new app_name: 'RubyTransitiveClosure'
  sc.set_log_level 'WARN'
  partitions = (ARGV[0] || 2).to_i

  graph = generate_graph
  # p graph
  tc = sc.parallelize_pairs(graph, partitions).cache
  edges = tc.map_to_pair{|x_y| tuple(x_y[1], x_y[0])}

  old_count = 0
  next_count = tc.count

  loop do
    old_count = next_count
    new_edges = tc.join(edges).map_to_pair{|__a_b| tuple(__a_b[1][1], __a_b[1][0])}
    tc = tc.union(new_edges).distinct.cache
    next_count = tc.count

    break if next_count == old_count
    $stderr.puts "old_count: #{old_count}, next_count: #{next_count}"
  end

  puts "TC has #{next_count} edges"

  sc.stop
end

main if JRubySpark.main?
