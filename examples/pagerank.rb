#!/usr/bin/env ruby

require 'jruby_spark/jruby_spark'

include JRubySpark

def main
  if ARGV.size != 2
    $stderr.puts "Usage: pagerank.rb <file> <iterations>"
    exit 1
  end

  $stderr.puts <<-EOF
WARN: This is a naive implementation of PageRank and is
given as an example! Please refer to PageRank implementation provided by graphx
  EOF

  conf = SparkConf.new
  conf.set_app_name 'rubyPagerank'
  sc = SparkContext.new conf

  # Loads in input file. It should be in format of:
  #     URL         neighbor URL
  #     URL         neighbor URL
  #     URL         neighbor URL
  #     ...
  lines = sc.text_file ARGV[0]

  # Loads all URLs from input file and initialize their neighbors.
  links = lines.map_to_pair{|x|
    url, n = x.split
    tuple(url, n)
  }.distinct.group_by_key.cache

  # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
  ranks = links.map_to_pair{|url_neighbors| tuple(url_neighbors[0], 1.0) }

  compute_contribs = -> (urls, rank) {
      num_urls = urls.size
      urls.map{|url| tuple(url, rank / num_urls)}
  }

  # Calculates and updates URL ranks continuously using PageRank algorithm.
  ARGV[1].to_i.times do
    # Calculates URL contributions to the rank of other URLs.
    contribs = links.join(ranks).flat_map_to_pair{ |url_urls_rank|
      compute_contribs.call(url_urls_rank[1][0], url_urls_rank[1][1])
    }
    ranks = contribs.reduce_by_key(:+).map_values{|rank| rank * 0.85 + 0.15}
  end

  ranks.collect.each do |r|
    puts "#{r[0]} has rank: #{r[1]}"
  end
end

main if JRubySpark.main?
