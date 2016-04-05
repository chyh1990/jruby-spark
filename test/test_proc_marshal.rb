require 'test/unit'
require 'jruby_spark/jruby_spark'

module CallHelper

  def _check_call f, args
    ret = f.call *args
    bytes = Marshal.dump(f)
    f1 = Marshal.load(bytes)
    assert Proc === f1
    assert_equal f.lambda?, f1.lambda?
    [ret, f1]
  end

  # eval the proc outside its original closure
  def check_call f, args = []
    ret, f1 = _check_call f, args
    ret1 = f1.call *args
    assert_equal ret, ret1
    ret1
  end
end

class ProcMarshalTest < Test::Unit::TestCase
  include CallHelper

  def setup
  end

  def test_simple
    f = lambda {|x| x+1}
    assert_equal check_call(f, [1]), 2

    f1 = Proc.new {|x| x+1}
    assert_equal check_call(f1, [1]), 2
  end

  def test_simple_closure
    cvar1 = 10
    f = lambda {|x| x+cvar1}
    assert_equal check_call(f, [10]), 20

    f = Proc.new {|x| x+cvar1}
    assert_equal check_call(f, [10]), 20
  end

  def test_nesting_block
    f = lambda {|x, y|
      x.map{|z|
        z + y
      }.inject(&:+)
    }
    assert_equal check_call(f, [[1,2,3], 10]), 36
  end

  def test_nesting_closure
    clv1 = 'xx'
    f = lambda {|x|
      ['a','b'].map do |y|
        y + clv1
      end
    }
    assert_equal check_call(f, ['c']), ['axx', 'bxx']

    f1 = lambda {|x|
      clv1.nil?
      ['a','b'].map do |y|
        y + clv1
      end
    }
    assert_equal check_call(f, ['c']), ['axx', 'bxx']


    closure = Proc.new do |v|
      clv2 = 'yy'
      lambda {|x|
        ['a','b'].map do |y|
          y + clv1 + clv2
        end
      }
    end
    bytes = Marshal.dump(closure)
    closure_ = Marshal.load(bytes)
    assert_equal closure_.call('v').call('c'), closure.call('v').call('c')
  end

  class CustomType
    attr_reader :v
    def initialize v
      @v = v
    end
  end

  def test_custom_type
    cvar = CustomType.new 'aaa'
    f = lambda {
      cvar.v + 'bbb'
    }
    assert_equal check_call(f), 'aaabbb'
  end

  def test_for_block
    f = lambda do |e|
      cnt = 0
      for i in e
        cnt += i
      end
      cnt
    end
    assert_equal check_call(f, [1..10]), 55
  end

  def test_block_method
    f = Proc.new do |u, v|
      def method1 x, y
        x + y
      end
      method1(u, v) + 10
    end
    assert_equal check_call(f, [1,2]), 13
  end
end

