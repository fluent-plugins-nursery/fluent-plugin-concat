module Fluent
  class ConcatFilter < Filter
    Plugin.register_filter("concat", self)

    desc "The key for part of multiline log"
    config_param :key, :string, required: true
    desc "The separator of lines"
    config_param :separator, :string, default: "\n"
    desc "The number of lines"
    config_param :n_lines, :integer, default: nil
    desc "The regexp to match beginning of multiline"
    config_param :multiline_start_regexp, :string, default: nil
    desc "The key to determine which stream an event belongs to"
    config_param :stream_identity_key, :string, default: nil

    def initialize
      super

      @buffer = Hash.new{|h, k| h[k] = [] }
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
      if @stream_identity_key
        stream_identity = record[@stream_identity_key]
      else
        stream_identity = "default"
      end
      case @mode
      when :line
        @buffer[stream_identity] << record[@key]
        if @n_lines && @buffer[stream_identity].size >= @n_lines
          return flush_buffer(stream_identity)
        end
      when :regexp
        if firstline?(record[@key])
          if @buffer[stream_identity].empty?
            @buffer[stream_identity] << record[@key]
          else
            return flush_buffer(stream_identity, record[@key])
          end
        else
          @buffer[stream_identity] << record[@key]
        end
      end
      nil
    end

    def firstline?(text)
      !!@multiline_start_regexp.match(text)
    end

    def flush_buffer(stream_identity, new_message = nil)
      new_record = {}
      new_record[@key] = @buffer[stream_identity].join(@separator)
      @buffer[stream_identity] = []
      @buffer[stream_identity] << new_message if new_message
      new_record
    end
  end
end
