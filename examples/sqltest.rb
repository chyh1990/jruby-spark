#!/usr/bin/env ruby

require 'jruby_spark/jruby_spark'

include JRubySpark

class Person < Schema
  field :name, String
  field :age, Integer
end

def main
  raise 'no input file' unless ARGV[0]
  conf = SparkConf.new
  conf.set_app_name 'person'
  $sc = SparkContext.new conf
  $sql = SQLContext.new $sc

  rdd = $sc.text_file(ARGV[0]).map{|e|
    t = e.split
    Person.create t[0], t[1].to_i
  }
  $df = $sql.create_data_frame rdd, Person
  $df.filter("age > 10").show

  # require 'pry'
  # pry.binding
end

main if JRubySpark.main?
