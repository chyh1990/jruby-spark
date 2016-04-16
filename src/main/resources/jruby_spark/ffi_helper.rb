require 'jruby_spark/jruby_spark'
require 'ffi'

module FFI
  module Library
    alias_method :ffi_lib_old, :ffi_lib
    def ffi_lib(*names)
      begin
        ffi_lib_old *names
      rescue LoadError => e
        # fallback
        spark_root = JRubySpark::SparkFiles.getRootDirectory rescue nil
        raise e unless spark_root

        raise LoadError.new("library names list must not be empty") if names.empty?

        lib_flags = defined?(@ffi_lib_flags) ? @ffi_lib_flags : FFI::DynamicLibrary::RTLD_LAZY | FFI::DynamicLibrary::RTLD_LOCAL
        ffi_libs = names.map do |name|
          libnames = (name.is_a?(::Array) ? name : [ name ])
            .map { |n| [ n, FFI.map_library_name(n) ].uniq }.flatten.compact
          lib = nil
          libnames.each do |libname|
            ['native', 'lib', '.'].each do |e|
              begin
                f = File.join spark_root, e, libname
                $stderr.puts "FFI: trying #{f}"
                next unless File.file? f
                lib = FFI::DynamicLibrary.open(f, lib_flags)
                break if lib
              rescue
                next
              end
            end

            break if lib
          end

          raise LoadError.new("library #{name} not found in spark temp") unless lib
          lib
        end
      end

      @ffi_libs = ffi_libs
    end
  end
end

