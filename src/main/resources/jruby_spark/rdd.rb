module JRubySpark

  class RDDBase < Delegator
    def initialize jrdd
      super
    end

    def __getobj__
      @jrdd
    end

    def __setobj__(obj)
      @jrdd = obj
    end

    protected
    def callJava method, fclazz, f, args = []
      args = [create_func!(fclazz, f)] + args
      @jrdd.__send__(method, *args)
    end

    def create_func! fclazz, f
      raise RDDError, 'not a lambda' unless Proc === f || Symbol === f
      payload = Marshal.dump(f).to_java_bytes
      fclazz.new(payload)
    end

  end

  class RDD < RDDBase
  end

  class PairRDD < RDDBase
  end

  class DoubleRDD < RDDBase
  end

  module DefHelper
    # create delegator method as:
    # def map f = nil, &block
    #   f ||= Proc.new(block) if block_given?
    #   callJava :map, JFunction, f
    # end
    def def_transform name, fclazz, ret_clazz = RDD, default_func = nil
      define_method(name) do |f = default_func, &block|
        f ||= block if block
        if ret_clazz
          ret_clazz.new(callJava(name, fclazz, f))
        else
          callJava(name, fclazz, f)
        end
      end
    end

    def wrap_return name, ret_clazz = self
      define_method(name) do |*args|
        ret_clazz.new(@jrdd.__send__(name, *args))
      end
    end

    def merge_rdd name, from_clazz = self, to_clazz = self
      define_method(name) do |rdd, *args|
        raise RDDError, "not a #{from_clazz}" unless rdd.is_a? from_clazz
        to_clazz.new(@jrdd.__send__(name, rdd.__getobj__, *args))
      end
    end
  end

  module RDDLike
    extend DefHelper

    def aggregate zero, seqOp, combOp
      @jrdd.__send__(:aggregate, zero, create_func!(JFunction2, seqOp), create_func!(JFunction2, combOp))
    end

    merge_rdd :cartesian, RDD, PairRDD

    def collect
      @jrdd.collect.map {|e| Helpers.to_ruby e}
    end

    def_transform :flat_map, JFlatMapFunction
    def_transform :flat_map_to_double, JFlatMapFunction, DoubleRDD
    def_transform :flat_map_to_pair, JPairFlatMapFunction, PairRDD

    def fold zero, f = nil, &block
      f ||= block if block
      RDD.new @jrdd.fold(zero, create_func!(JFunction2, f))
    end

    def foreach_partition f = nil, &block
      f ||= block if block
      proxy_f = lambda {|__it|
        __rb_it = __it.lazy.map{|x| Helpers.to_ruby x}
        f.call(__rb_it)
      }
      RDD.new @jrdd.foreachPartition(create_func!(JVoidFunction, proxy_f))
    end

    wrap_return :glom, RDD

    def group_by f = nil, num_partitions = nil, &block
      f ||= block if block
      if num_partitions
        PairRDD.new @jrdd.groupBy(create_func!(JFunction, f), num_partitions)
      else
        PairRDD.new @jrdd.groupBy(create_func!(JFunction, f))
      end
    end

    def empty?
      @jrdd.isEmpty
    end

    def_transform :key_by, JFunction, PairRDD

    def_transform :reduce, JFunction2, nil
    def_transform :foreach, JVoidFunction, nil
    def_transform :map, JFunction
    def_transform :map_to_double, JDoubleFunction, DoubleRDD, :to_f
    def_transform :map_to_pair, JPairFunction, PairRDD

    def map_partitions_with_index f = nil, preservesPartitioning = false, &block
      f ||= block if block
      to_iter_f2 = lambda {|v1, v2| Helpers.to_iter f.call(v1, v2) }
      RDD.new @jrdd.mapPartitionsWithIndex(create_func!(JFunction2, to_iter_f2), preservesPartitioning)
    end

    def map_partitions f = nil, preservesPartitioning = false, &block
      f ||= block if block
      proxy_f = lambda {|__it|
        __rb_it = __it.lazy.map{|x| Helpers.to_ruby x}
        f.call(__rb_it)
      }
      RDD.new @jrdd.mapPartitions(create_func!(JFlatMapFunction, proxy_f), preservesPartitioning)
    end

    def tree_aggregate zero, seqOp, combOp, depth = 2
      @jrdd.treeAggregate zero, create_func!(JFunction2, seqOp), create_func!(JFunction2, combOp), depth
    end

    def tree_reduce zero, f = nil, depth = 2, &block
      f ||= block if block
      @jrdd.treeReduce zero, create_func!(JFunction2, f), depth
    end

    def zip(other)
      PairRDD.new(@jrdd.zip(other.__getobj__))
    end

    def zip_with_index
      PairRDD.new @jrdd.zipWithIndex
    end

    def zip_with_unique_id
      PairRDD.new @jrdd.zipWithUniqueId
    end

    def inspect
      "#<#{self.class}: #{@jrdd.inspect}>"
    end
  end

  class RDD
    extend DefHelper
    include RDDLike
    # def filter &block
    #   f = Proc.new block
    #   wrap = lambda {|x|
    #     f.call(x) ? true.to_java : false.to_java
    #   }
    #   RDD.new(callJava(:filter, JFunction, wrap))
    # end

    merge_rdd :intersection

    wrap_return :cache
    wrap_return :coalesce
    wrap_return :distinct
    wrap_return :persist
    def_transform :filter, JFunction

    def random_split weights, seed = rand(100000000)
      @jrdd.randomSplit(weights, seed).map{|e| RDD.new(e)}
    end

    wrap_return :repartition
    wrap_return :sample

    def sort_by f = nil, ascending = true, num_partitions = nil, &block
      f ||= block if block
      num_partitions ||= @jrdd.getNumPartitions
      RDD.new(@jrdd.sortBy(create_func!(JFunction, f), ascending, num_partitions))
    end

    merge_rdd :substract
    merge_rdd :union
    wrap_return :unpersist

    def to_double_rdd
      DoubleRDD.from_rdd self
    end
  end

  class DoubleRDD
    extend DefHelper
    include RDDLike

    def self.from_rdd rdd
      raise RDDError, 'not an rdd' unless RDD === rdd
      @jrdd = JavaDoubleRDD.fromRDD rdd.__getobj__.rdd
    end

    wrap_return :cache
    wrap_return :coalesce
    wrap_return :distinct
    def_transform :filter, JFunction

    merge_rdd :intersection
    wrap_return :persist
    wrap_return :repartition
    wrap_return :sample

    merge_rdd :substract
    merge_rdd :union
    wrap_return :unpersist

  end

  class PairRDD
    extend DefHelper
    include RDDLike

    # TODO aggregatebykey
    wrap_return :cache
    wrap_return :coalesce
    merge_rdd :cogroup, PairRDD, PairRDD

    def collect_as_map
      # XXX how to safely sanity key/values
      h = Hash.new
      @jrdd.collectAsMap.each {|e| h[Helpers.to_ruby(e[0])] = Helpers.to_ruby(e[1])}
      h
    end
    def cogroup *args
      case args.size
      when 1
        PairRDD.new(@jrdd.cogroup(args[0].__getobj__))
      when 2
        if PairRDD === args[1]
          PairRDD.new(@jrdd.cogroup(args[0].__getobj__), args[1].__getobj__)
        else
          PairRDD.new(@jrdd.cogroup(args[0].__getobj__), args[1])
        end
      when 3
        if PairRDD === args[2]
          PairRDD.new(@jrdd.cogroup(args[0].__getobj__), args[1].__getobj__, args[2].__getobj__)
        else
          PairRDD.new(@jrdd.cogroup(args[0].__getobj__), args[1].__getobj__, args[2])
        end
      when 4
          PairRDD.new(@jrdd.cogroup(args[0].__getobj__), args[1].__getobj__, args[2].__getobj__, args[3])
      else
        raise ArgumentError('unexpected argument numbers')
      end
    end

    def combine_by_key(combiner, mergeV, mergeC, num_partitions = nil)
      if num_partitions
        # TODO more overrides
        PairRDD.new(@jrdd.combineByKey(create_func!(JFunction, combiner),
                                       create_func!(JFunction2, mergeV),
                                       create_func!(JFunction2, mergeC),
                                       num_partitions))
      else
        PairRDD.new(@jrdd.combineByKey(create_func!(JFunction, combiner),
                                       create_func!(JFunction2, mergeV),
                                       create_func!(JFunction2, mergeC),
                                      ))
      end
    end

    wrap_return :distinct
    def_transform :filter, JFunction, PairRDD
    def_transform :flat_map_values, JFunction, PairRDD

    def fold_by_key(zero, num_partitions = nil, f = nil, &block)
      f ||= block if block
      num_partitions ||= @jrdd.getNumPartitions
      PairRDD.new(@jrdd.foldByKey(zero, num_partitions, create_func!(JFunction2, f)))
    end

    merge_rdd :full_outer_join
    wrap_return :group_by_key

    alias_method :group_with, :cogroup
    merge_rdd :join
    # TODO keys
    wrap_return :left_outer_join

    def_transform :map_values, JFunction, PairRDD
    wrap_return :persist

    def_transform :reduce_by_key, JFunction2, PairRDD

    def reduce_by_key_locally
      @jrdd.reduceByKeyLocally(create_func!(JFunction2, f))
    end

    wrap_return :repartition
    wrap_return :repartition_and_sort_within_partitions

    merge_rdd :right_outer_join
    wrap_return :sample
    wrap_return :sample_by_key
    wrap_return :sample_by_key_exact
    wrap_return :sort_by_key
    merge_rdd :substract
    merge_rdd :substract_by_key
    merge_rdd :union
    wrap_return :unpersist
    wrap_return :values, RDD
  end

  class Broadcast
    java_import org.apache.spark.jruby.TypeUtils
    def initialize(sc, v)
      t = TypeUtils.rubyToJava(JRuby.runtime, v)
      @jv = sc.__getobj__.broadcast(t)
    end

    def unpersist(*args)
      @jv.unpersist *args
    end

    def destroy
      @jv.destroy
    end

    def value
      v = @jv.value
      TypeUtils.javaToRuby(JRuby.runtime, v)
    end
  end

  class SparkContext < Delegator

    def self.wrap_return name, ret_clazz
      define_method(name) do |*args|
        ret_clazz.new(@jctx.__send__(name, *args))
      end
    end

    def initialize arg
      if Hash === arg
        conf = SparkConf.new
        arg.each {|k, v|
          m1 = "set_#{arg}".to_sym
          m2 = "set#{arg}".to_sym
          if conf.respond_to? m1
            conf.__send__(m1.to_sym, v)
          elsif conf.respond_to? m2
            conf.__send__(m2.to_sym, v)
          else
            conf.set(k.to_s, v)
          end
        }
      elsif SparkConf === arg
        conf = arg
      else
        raise ArgumentError, 'argument should be hash or SparkConf'
      end
      ctx = JavaSparkContext.new conf
      @jctx = ctx
    end

    def __getobj__
      @jctx
    end

    wrap_return :text_file, RDD
    wrap_return :whole_text_files, RDD
    wrap_return :hadoop_file, PairRDD
    wrap_return :hadoop_rdd, PairRDD
    wrap_return :new_api_hadoop_rdd, PairRDD
    wrap_return :object_file, RDD
    wrap_return :sequence_file, PairRDD

    def parallelize(e, num_partitions = nil)
      if num_partitions
        RDD.new @jctx.parallelize(e.to_a, num_partitions)
      else
        RDD.new @jctx.parallelize(e.to_a)
      end
    end

    def parallelize_doubles(e, num_partitions = nil)
      t = e.to_a.to_java(:double)
      if num_partitions
        DoubleRDD.new @jctx.parallelizeDoubles(t, num_partitions)
      else
        DoubleRDD.new @jctx.parallelizeDoubles(t)
      end
    end

    def parallelize_pairs(e, num_partitions = nil)
      if num_partitions
        PairRDD.new @jctx.parallelizePairs(e.to_a, num_partitions)
      else
        PairRDD.new @jctx.parallelizePairs(e.to_a)
      end
    end

    def union *args
      raise ArgumentError, 'empty set' if args.empty?
      clazz = args.first
      raise ArgumentError, 'RDD expected' unless clazz.is_a? RDDLike
      args.map!{|x| x.__getobj__}
      first = args.shift
      clazz.class.new(@jctx.union(first, args))
    end

    def accumulator inital_value, name = nil
      args = [inital_value]
      args << name if name
      # always use long accumulator
      if Fixnum === inital_value
        args << Java::OrgApacheSparkJruby::JLongAccumulatorParam.new
      end
      @jctx.__send__(:accumulator, *args)
    end

    def broadcast v
      Broadcast.new self, v
    end

    def inspect
      "#<#{self.class} jctx=#{@jctx.inspect}>"
    end
  end

end
