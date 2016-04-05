require 'logger'
require 'webrick'
require 'fileutils'

begin
  require 'pry'
rescue LoadError
  $stderr.puts "pry gem is not installed."
  exit 1
end

require 'jruby_spark/jruby_spark'

module REPLTopLevel
  include JRubySpark

end

module JRubySpark
  class REPL
    attr_reader :code_cache, :server
    def initialize
      @code_cache = []
      @require_cache = []
      @last_eval = nil
      @logger = Logger.new $stderr
      @tmpdir = Dir.mktmpdir('jruby-spark-repl-')
      @logger.info "HTTP dir: #{@tmpdir}"
      dev_null = File.open(File::NULL, 'w+')
      @server ||= WEBrick::HTTPServer.new(:Port => 61637,
                                          :DocumentRoot => @tmpdir,
                                          #:Logger => dev_null,
                                          :AccessLog => dev_null
      )
    end

    def dump io
      io.puts <<-PROLOG
require 'jruby_spark/jruby_spark'

module REPLTopLevel
end
      PROLOG

      @require_cache.each do |e|
        io.puts e
      end
      io.puts

      # REPL lambda do not defined in REPLTopLevel
      @code_cache.each do |e|
        io.puts e[:code]
      end

      # user defined modules
      # TODO constants
      REPLTopLevel.constants(false).each do |c|
        # io.puts c.source
        # v = REPLTopLevel.const_get(c)
      end

      io.puts
    end

    def add_code res
      # p "RESULT: #{res}"
      line = @last_eval.split("\n").first
      return unless line
      line.strip!
      if line =~ /^(class|module|def)\s+([a-zA-Z0-9_?!:]+)/
        type = $1
        name = $2
        @code_cache << {name: name, type: type.to_s, code: @last_eval}
      elsif line =~ /^require\s+/
        @require_cache << line
      end
    end

    def set_code code
      @last_eval = code
      File.open(File.join(@tmpdir, '_repl_main.rb'), 'w') do |f|
        dump f
      end
    end

    def start_http
      Thread.new do
        @logger.info "Starting REPL HTTP Server: http://0.0.0.0:61637/"
        @server.start
      end
    end

    def shutdown
      @server.shutdown
      FileUtils.remove_entry @tmpdir
    end

    def start
      inst = self
      start_http

      Pry.config.hooks.add_hook(:before_eval, :set_code) do |src|
        inst.set_code src
      end
      Pry.config.hooks.add_hook(:after_eval, :add_code) do |res|
        inst.add_code res
      end
      Pry.config.hooks.add_hook(:after_session, :shutdown_jspark) do |res|
        inst.shutdown
      end

      command_set = Pry::CommandSet.new do
        command 'spark-remove' do |name|
          # output.puts "hello #{name}"
          REPLTopLevel.__send__(:remove_const, name.to_sym)
        end

        command 'spark-dump' do
          inst.dump $stdout
          # p inst.code_cache
        end
      end

      Pry.start REPLTopLevel, commands: command_set
    end
  end
end

if __FILE__ == $0
  repl = JRubySpark::REPL.new

  conf = JRubySpark::SparkConf.new
  conf.setMaster('local').setAppName("spark-repl-#{Time.now.to_i}")
  conf.set('spark.executor.jruby.bootstrap_file', 'classpath:jruby_spark/repl_executor.rb')
  conf.set('spark.executor.jruby.repl_http', 'XXX')

  $sc = JRubySpark::SparkContext.new conf
  $sc.setLogLevel("WARN")

  logo = <<-'LOGO'
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version $version
      /_/
  LOGO
  puts logo.gsub('$version', $sc.version.to_s)
  puts "Using Ruby #{RUBY_VERSION}(#{RUBY_PLATFORM})"
  puts "SparkContext available as $sc"

  repl.start
end
