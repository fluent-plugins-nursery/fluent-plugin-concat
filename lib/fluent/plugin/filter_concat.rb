module Fluent
  class ConcatFilter < Filter
    Plugin.register_filter("concat", self)

    desc "The key"
    config_param :key, :string, required: true
    desc "The separator of lines"
    config_param :separator, :string, default: "\n"
    desc "The number of lines"
    config_param :n_lines, :integer, default: nil
    desc "The max number of lines"
    config_param :max_n_lines, :integer, default: 1000
    desc "The interval of flushing the buffer"
    config_param :flush_interval, :time, default: nil
    desc "The regexp to match beginning of multiline"
    config_param :multiline_start_regexp, :string, default: nil

    def initialize
      super

      @buffer = []
    end

    def configure(conf)
      super

      if @n_lines && @multiline_start_regexp
        raise ConfigError, "n_lines and multiline_start_regexp are exclusive"
      end
      if @n_lines.nil? && @multiline_start_regexp.nil?
        raise ConfigError, "Either n_lines or multiline_start_regexp is required"
      end

      @mode = nil
      case
      when @n_lines
        @mode = :line
      when @multiline_start_regexp
        @multiline_start_regexp = Regexp.compile(@multiline_start_regexp[1..-2])
        @mode = :regexp
      end
    end

    def filter_stream(tag, es)
      new_es = MultiEventStream.new
      es.each do |time, record|
        begin
          new_record = process(record)
          new_es.add(time, record.merge(new_record)) if new_record
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      end
      new_es
    end

    private

    def process(record)
      case @mode
      when :line
        @buffer << record[@key]
        if @n_lines && @buffer.size >= @n_lines
          new_record = {}
          new_record[@key] = @buffer.join(@separator)
          @buffer = []
          return new_record
        end
      when :regexp
        if firstline?(record[@key])
          if @buffer.empty?
            @buffer << record[@key]
          else
            new_record = {}
            new_record[@key] = @buffer.join(@separator)
            @buffer = []
            return new_record
          end
        else
          @buffer << record[@key]
        end
      end
      nil
    end

    def firstline?(text)
      !!@multiline_start_regexp.match(text)
    end
  end
end
