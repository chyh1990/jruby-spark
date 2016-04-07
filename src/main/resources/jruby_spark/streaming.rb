require 'jruby_spark/rdd'

module JRubySpark
  java_import org.apache.spark.streaming.api.java.JavaStreamingContext
  java_import org.apache.spark.streaming.Duration

  class DStream < RDDBase
  end

  class PairDStream < RDDBase
  end

  module DStreamLike
    extend DefHelper

    wrap_return :checkpoint, DStream
    wrap_return :count, DStream
    wrap_return :count_by_value, PairDStream
    wrap_return :count_by_value_and_window, PairDStream
    wrap_return :count_by_window, DStream
    def_transform :flat_map, JFlatMapFunction, DStream
    def_transform :flat_map_to_pair, JFlatMapFunction, PairDStream
    def_transform :foreach_rdd, JVoidFunction, nil
    #def_transform :foreach_rdd_with_time, JVoidFunction2, nil
    wrap_return :glom, DStream
    def_transform :map, JFunction, DStream
    def_transform :map_partitions, JFlatMapFunction, DStream
    def_transform :map_partitions_to_pair, JFlatMapFunction, PairDStream
    def_transform :map_to_pair, JPairFunction, PairDStream
    def_transform :reduce, JFunction2, DStream

    # TODO  reduceByWindow
    def_transform :transform, JFunction, DStream
    def_transform :transform_with_time, JFunction2, DStream

    # TODO
    # more transform
  end

  class DStream
    include RDDLike
    include DStreamLike

    extend DefHelper

    wrap_return :cache
    wrap_return :compute, RDD
    def_transform :filter, JFunction, DStream
    wrap_return :persist
    wrap_return :repartition
    merge_rdd :union, DStream, DStream
    wrap_return :window
  end

  class PairDStream
    include RDDLike
    include DStreamLike

    extend DefHelper

  end

  class StreamingContext < Delegator
    def initialize sc, duration_ms = 1000
      ctx = JavaStreamingContext.new $sc.__getobj__, Duration.new(duration_ms)
      @jctx = ctx
    end

    def __getobj__
      @jctx
    end

    def inspect
      "#<#{self.class} jctx=#{@jctx.inspect}>"
    end

    def socket_text_stream hostname, port, storageLevel = nil
      if storageLevel
        DStream.new @jctx.socketTextStream(hostname, port, storageLevel)
      else
        DStream.new @jctx.socketTextStream(hostname, port)
      end
    end
  end
end
