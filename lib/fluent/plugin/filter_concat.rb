require "fluent/plugin/filter"

module Fluent::Plugin
  class ConcatFilter < Filter
    Fluent::Plugin.register_filter("concat", self)

    helpers :timer, :event_emitter

    desc "The key for part of multiline log"
    config_param :key, :string
    desc "The separator of lines"
    config_param :separator, :string, default: "\n"
    desc "The number of lines"
    config_param :n_lines, :integer, default: nil
    desc "The regexp to match beginning of multiline"
    config_param :multiline_start_regexp, :string, default: nil
    desc "The regexp to match ending of multiline"
    config_param :multiline_end_regexp, :string, default: nil
    desc "The regexp to match continuous lines"
    config_param :continuous_line_regexp, :string, default: nil
    desc "The key to determine which stream an event belongs to"
    config_param :stream_identity_key, :string, default: nil
    desc "The interval between data flushes, 0 means disable timeout"
    config_param :flush_interval, :time, default: 60
    desc "The label name to handle events caused by timeout"
    config_param :timeout_label, :string, default: nil
    desc "Use timestamp of first record when buffer is flushed"
    config_param :use_first_timestamp, :bool, default: false
    desc "The field name that is the reference to concatenate records"
    config_param :partial_key, :string, default: nil
    desc "The value stored in the field specified by partial_key that represent partial log"
    config_param :partial_value, :string, default: nil
    desc "If true, keep partial_key in concatenated records"
    config_param :keep_partial_key, :bool, default: false
    desc "Use partial metadata to concatenate multiple records"
    config_param :use_partial_metadata, :bool, default: false
    desc "Input format of the partial metadata (fluentd or journald docker log driver)"
    config_param :partial_metadata_format, :enum, list: [:"docker-fluentd", :"docker-journald", :"docker-journald-lowercase"], default: :"docker-fluentd"
    desc "If true, keep partial metadata"
    config_param :keep_partial_metadata, :bool, default: false
    desc "Use cri log tag to concatenate multiple records"
    config_param :use_partial_cri_logtag, :bool, default: false
    desc "The key name that is referred to concatenate records on cri log"
    config_param :partial_cri_logtag_key, :string, default: nil
    desc "The key name that is referred to detect stream name on cri log"
    config_param :partial_cri_stream_key, :string, default: "stream"
    desc "The max size of each buffer"
    config_param :buffer_limit_size, :size, default: 500 * 1024 # 500k
    desc "The method if overflow buffer"
    config_param :buffer_overflow_method, :enum, list: [:ignore, :truncate, :drop, :new], default: :ignore

    class TimeoutError < StandardError
    end

    def initialize
      super

      @buffer = Hash.new {|h, k| h[k] = [] }
      @buffer_size = Hash.new(0)
      @timeout_map_mutex = Thread::Mutex.new
      @timeout_map_mutex.synchronize do
        @timeout_map = Hash.new {|h, k| h[k] = Fluent::Engine.now }
      end
    end

    def required_params
      params = [@n_lines.nil?, @multiline_start_regexp.nil?, @multiline_end_regexp.nil?, @partial_key.nil?, !@use_partial_metadata, !@use_partial_cri_logtag]
      names = ["n_lines", "multiline_start_regexp", "multiline_end_regexp", "partial_key", "use_partial_metadata", "use_partial_cri_logtag"]
      return params, names
    end

    def configure(conf)
      super

      params, names = required_params
      if params.all?
        raise Fluent::ConfigError, "Either #{[names[0..-2].join(", "), names[-1]].join(" or ")} is required"
      end
      if @n_lines && (@multiline_start_regexp || @multiline_end_regexp)
        raise Fluent::ConfigError, "n_lines and multiline_start_regexp/multiline_end_regexp are exclusive"
      end
      if @partial_key && @n_lines
        raise Fluent::ConfigError, "partial_key and n_lines are exclusive"
      end
      if @partial_key && (@multiline_start_regexp || @multiline_end_regexp)
        raise Fluent::ConfigError, "partial_key and multiline_start_regexp/multiline_end_regexp are exclusive"
      end
      if @partial_key && @partial_value.nil?
        raise Fluent::ConfigError, "partial_value is required when partial_key is specified"
      end
      if @use_partial_metadata && @n_lines
        raise Fluent::ConfigError, "use_partial_metadata and n_lines are exclusive"
      end
      if @use_partial_metadata && (@multiline_start_regexp || @multiline_end_regexp)
        raise Fluent::ConfigError, "use_partial_metadata and multiline_start_regexp/multiline_end_regexp are exclusive"
      end
      if @use_partial_metadata && @partial_key
        raise Fluent::ConfigError, "use_partial_metadata and partial_key are exclusive"
      end
      if @use_partial_cri_logtag && @n_lines
        raise Fluent::ConfigError, "use_partial_cri_logtag and n_lines are exclusive"
      end
      if @use_partial_cri_logtag && (@multiline_start_regexp || @multiline_end_regexp)
        raise Fluent::ConfigError, "use_partial_cri_logtag and multiline_start_regexp/multiline_end_regexp are exclusive"
      end
      if @use_partial_cri_logtag && @partial_key
        raise Fluent::ConfigError, "use_partial_cri_logtag and partial_key are exclusive"
      end

      @mode = nil
      case
      when @n_lines
        @mode = :line
      when @partial_key
        @mode = :partial
      when @use_partial_metadata
        @mode = :partial_metadata
        case @partial_metadata_format
        when :"docker-fluentd"
          @partial_message_field     = "partial_message".freeze
          @partial_id_field          = "partial_id".freeze
          @partial_ordinal_field     = "partial_ordinal".freeze
          @partial_last_field        = "partial_last".freeze
          @partial_message_indicator = @partial_message_field
        when :"docker-journald"
          @partial_message_field     = "CONTAINER_PARTIAL_MESSAGE".freeze
          @partial_id_field          = "CONTAINER_PARTIAL_ID".freeze
          @partial_ordinal_field     = "CONTAINER_PARTIAL_ORDINAL".freeze
          @partial_last_field        = "CONTAINER_PARTIAL_LAST".freeze
          # the journald log driver does not add CONTAINER_PARTIAL_MESSAGE to the last message
          # so we help ourself by using another indicator
          @partial_message_indicator = @partial_id_field
        when :"docker-journald-lowercase"
          @partial_message_field     = "container_partial_message".freeze
          @partial_id_field          = "container_partial_id".freeze
          @partial_ordinal_field     = "container_partial_ordinal".freeze
          @partial_last_field        = "container_partial_last".freeze
          @partial_message_indicator = @partial_id_field
        end
      when @use_partial_cri_logtag
        @mode = :partial_cri
        if @partial_cri_logtag_key.nil?
          raise Fluent::ConfigError, "partial_cri_logtag_key is required when use_partial_cri_logtag is specified"
        end
        @partial_logtag_delimiter = ":".freeze
        @partial_logtag_continue = "P".freeze
        @partial_logtag_full = "F".freeze
      when @multiline_start_regexp || @multiline_end_regexp
        @mode = :regexp
        if @multiline_start_regexp
          @multiline_start_regexp = Regexp.compile(@multiline_start_regexp[1..-2])
        end
        if @multiline_end_regexp
          @multiline_end_regexp = Regexp.compile(@multiline_end_regexp[1..-2])
        end
        if @continuous_line_regexp
          @continuous_line_regexp = Regexp.compile(@continuous_line_regexp[1..-2])
        end
      end
    end

    def start
      super
      @finished = false
      timer_execute(:filter_concat_timer, 1, &method(:on_timer))
    end

    def shutdown
      @finished = true
      flush_remaining_buffer
      super
    end

    def filter_stream(tag, es)
      if /\Afluent\.(?:trace|debug|info|warn|error|fatal)\z/ =~ tag
        return es
      end

      new_es = Fluent::MultiEventStream.new
      es.each do |time, record|
        unless record.key?(@key)
          new_es.add(time, record)
          next
        end
        if @mode == :partial
          unless record.key?(@partial_key)
            new_es.add(time, record)
            next
          end
        end
        if @mode == :partial_metadata
          unless record.key?(@partial_message_indicator)
            new_es.add(time, record)
            next
          end
        end
        if @mode == :partial_cri
          unless record.key?(@partial_cri_logtag_key)
            new_es.add(time, record)
            next
          end
        end
        begin
          flushed_es = process(tag, time, record)
          unless flushed_es.empty?
            flushed_es.each do |_time, new_record|
              time = _time if @use_first_timestamp
              merged_record = record.merge(new_record)
              case @mode
              when :partial
                merged_record.delete(@partial_key) unless @keep_partial_key
              when :partial_metadata
                unless @keep_partial_metadata
                  merged_record.delete(@partial_message_field)
                  merged_record.delete(@partial_id_field)
                  merged_record.delete(@partial_ordinal_field)
                  merged_record.delete(@partial_last_field)
                end
              when :partial_cri
                merged_record.delete(@partial_cri_logtag_key) unless @keep_partial_key
                merged_record.delete(@partial_cri_stream_key)
              end
              new_es.add(time, merged_record)
            end
          end
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      end
      new_es
    end

    private

    def on_timer
      return if @flush_interval <= 0
      return if @finished
      flush_timeout_buffer
    rescue => e
      log.error "failed to flush timeout buffer", error: e
    end

    def process(tag, time, record)
      # The key holds frozen copies, so a later change to the event's
      # Strings cannot change a key that is already stored.
      key_tag = tag.dup.freeze
      if @mode == :partial_metadata
        if @stream_identity_key
          # The identity and the partial ID are separate elements, so that
          # for example "web1" + "23" and "web12" + "3" are different streams.
          stream_identity = [key_tag, record[@stream_identity_key].to_s.dup.freeze, record[@partial_id_field].to_s.dup.freeze]
        else
          stream_identity = [key_tag, record[@partial_id_field].to_s.dup.freeze]
        end
      else
        if @stream_identity_key
          stream_identity = [key_tag, record[@stream_identity_key].to_s.dup.freeze]
        else
          stream_identity = [key_tag, "default"]
        end
      end
      # The timer thread flushes and purges @buffer in flush_timeout_buffer
      # under the same mutex, so updating the timestamp and the buffer must be
      # one step. Otherwise the timer can flush a stream between the two and
      # purge or read the buffer while this thread changes it.
      @timeout_map_mutex.synchronize do
        @timeout_map[stream_identity] = Fluent::Engine.now
        new_es = case @mode
                 when :line
                   process_line(stream_identity, tag, time, record)
                 when :partial
                   process_partial(stream_identity, tag, time, record)
                 when :partial_metadata
                   process_partial_metadata(stream_identity, tag, time, record)
                 when :partial_cri
                   process_partial_cri(stream_identity, tag, time, record)
                 when :regexp
                   process_regexp(stream_identity, tag, time, record)
                 end
        # A stream without buffered events has nothing left to flush, so its
        # state is removed here. Otherwise it stays until the timer purges
        # it, and with flush_interval 0 the timer never does, so the state
        # of every stream (one per split message in partial metadata mode)
        # would stay in memory forever.
        purge_stream_state(stream_identity) if @buffer[stream_identity].empty?
        new_es
      end
    end

    def purge_stream_state(stream_identity)
      @timeout_map.delete(stream_identity)
      @buffer.delete(stream_identity)
      @buffer_size.delete(stream_identity)
    end

    def process_line(stream_identity, tag, time, record)
      new_es = Fluent::MultiEventStream.new
      @buffer[stream_identity] << [tag, time, record]
      if @buffer[stream_identity].size >= @n_lines
        new_time, new_record = flush_buffer(stream_identity)
        time = new_time if @use_first_timestamp
        new_es.add(time, new_record)
      end
      new_es
    end

    def process_partial(stream_identity, tag, time, record)
      new_es = Fluent::MultiEventStream.new
      force_flush = false
      if overflow?(stream_identity, record)
        force_flush = case @buffer_overflow_method
                      when :ignore
                        @buffer[stream_identity] << [tag, time, record]
                        false
                      when :truncate
                        true
                      when :drop
                        @buffer[stream_identity] = []
                        false
                      when :new
                        true
                      end
      else
        @buffer[stream_identity] << [tag, time, record]
      end
      if force_flush || @partial_value != record[@partial_key]
        new_time, new_record = flush_buffer(stream_identity)
        time = new_time if @use_first_timestamp
        new_record.delete(@partial_key)
        new_es.add(time, new_record)
      end
      if force_flush && @buffer_overflow_method == :new
        @buffer[stream_identity] << [tag, time, record]
        @buffer_size[stream_identity] = record_bytesize(record)
        if @partial_value != record[@partial_key]
          new_time, new_record = flush_buffer(stream_identity)
          time = new_time if @use_first_timestamp
          new_record.delete(@partial_key)
          new_es.add(time, new_record)
        end
      end
      new_es
    end

    def process_partial_cri(stream_identity, tag, time, record)
      new_es = Fluent::MultiEventStream.new
      @buffer[stream_identity] << [tag, time, record]
      if record[@partial_cri_logtag_key].split(@partial_logtag_delimiter)[0] == @partial_logtag_full
        new_time, new_record = flush_buffer(stream_identity)
        time = new_time if @use_first_timestamp
        new_record.delete(@partial_cri_logtag_key)
        new_es.add(time, new_record)
      end
      new_es
    end

    def process_partial_metadata(stream_identity, tag, time, record)
      new_es = Fluent::MultiEventStream.new
      @buffer[stream_identity] << [tag, time, record]
      if record[@partial_last_field] == "true"
        new_time, new_record = flush_buffer(stream_identity)
        time = new_time if @use_first_timestamp
        new_record.delete(@partial_key)
        new_es.add(time, new_record)
      end
      new_es
    end

    def process_regexp(stream_identity, tag, time, record)
      new_es = Fluent::MultiEventStream.new
      case
      when firstline?(record[@key])
        if @buffer[stream_identity].empty?
          @buffer[stream_identity] << [tag, time, record]
          if lastline?(record[@key])
            new_time, new_record = flush_buffer(stream_identity)
            time = new_time if @use_first_timestamp
            new_es.add(time, new_record)
          end
        else
          new_time, new_record = flush_buffer(stream_identity, [tag, time, record])
          time = new_time if @use_first_timestamp
          new_es.add(time, new_record)
          if lastline?(record[@key])
            new_time, new_record = flush_buffer(stream_identity)
            time = new_time if @use_first_timestamp
            new_es.add(time, new_record)
          end
          return new_es
        end
      when lastline?(record[@key])
        @buffer[stream_identity] << [tag, time, record]
        new_time, new_record = flush_buffer(stream_identity)
        time = new_time if @use_first_timestamp
        new_es.add(time, new_record)
        return new_es
      else
        if @buffer[stream_identity].empty?
          if !@multiline_start_regexp
            @buffer[stream_identity] << [tag, time, record]
          else
            new_es.add(time, record)
            return new_es
          end
        else
          if continuous_line?(record[@key])
            # Continuation of the previous line
            @buffer[stream_identity] << [tag, time, record]
          else
            new_time, new_record = flush_buffer(stream_identity)
            # Only the flushed multiline takes the timestamp of its first
            # line. The current line is not part of it and keeps its own.
            new_es.add(@use_first_timestamp ? new_time : time, new_record)
            new_es.add(time, record)
          end
        end
      end
      new_es
    end

    def firstline?(text)
      @multiline_start_regexp && @multiline_start_regexp.match?(text)
    end

    def lastline?(text)
      @multiline_end_regexp && @multiline_end_regexp.match?(text)
    end

    def continuous_line?(text)
      if @continuous_line_regexp
        @continuous_line_regexp.match?(text)
      else
        true
      end
    end

    def overflow?(stream_identity, record)
      size = record_bytesize(record)
      if @buffer_size[stream_identity] + size > @buffer_limit_size
        @buffer_size[stream_identity] = 0
        true
      else
        @buffer_size[stream_identity] += size
        false
      end
    end

    def flush_buffer(stream_identity, new_element = nil, forced: false)
      elements = @buffer[stream_identity]
      lines = forced ? forced_buffered_lines(elements) : buffered_lines(elements)
      _tag, time, first_record = @buffer[stream_identity].first
      new_record = {
        @key => lines.join(@separator)
      }
      @buffer[stream_identity] = []
      @buffer[stream_identity] << new_element if new_element
      @buffer_size[stream_identity] = 0
      [time, first_record.merge(new_record)]
    end

    # The lines of the buffered events, in the order they are joined.
    # With partial metadata the parts can arrive out of order, so they are
    # sorted by their ordinal.
    def buffered_lines(elements)
      if @mode == :partial_metadata
        elements = elements.sort_by {|_tag, _time, record| record[@partial_ordinal_field].to_i }
      end
      elements.map {|_tag, _time, record| record[@key] }
    end

    # Same as buffered_lines, but never raises. A timeout flush or the flush
    # at shutdown must still flush every buffered stream, so when the
    # ordinals cannot be sorted (for example an ordinal that does not respond
    # to to_i) the parts are joined in the order they arrived.
    def forced_buffered_lines(elements)
      buffered_lines(elements)
    rescue StandardError
      elements.map {|_tag, _time, record| record[@key] }
    end

    def flush_timeout_buffer
      now = Fluent::Engine.now
      expired_stream_identities = []
      @timeout_map_mutex.synchronize do
        @timeout_map.each do |stream_identity, previous_timestamp|
          next if @flush_interval > (now - previous_timestamp)
          expired_stream_identities << stream_identity
          next if @buffer[stream_identity].empty?
          tag = @buffer[stream_identity].first[0]
          time, flushed_record = flush_buffer(stream_identity, forced: true)
          message = "Timeout flush: #{stream_identity_label(stream_identity)}"
          handle_timeout_error(tag, @use_first_timestamp ? time : now, flushed_record, message)
          log.info(message)
        end
        # Purge expired streams from all the state hashes: the streams flushed
        # above, and the empty state left behind when process or an earlier
        # timeout flush raised before the state of the stream was removed.
        expired_stream_identities.each do |stream_identity|
          purge_stream_state(stream_identity)
        end
      end
    end

    def flush_remaining_buffer
      # Another plugin of the same label can emit into this filter while it
      # shuts down, so process can add a stream to @buffer from another thread
      # during this flush. Iterating @buffer itself would make that thread
      # raise "can't add a new key into hash during iteration", so the streams
      # are taken from snapshots of the keys, until no new stream shows up.
      # The record is emitted outside the mutex, because an emit to a
      # timeout_label that leads back to this filter calls process again.
      flushed = {}
      loop do
        stream_identities = @timeout_map_mutex.synchronize { @buffer.keys }.reject {|stream_identity| flushed.key?(stream_identity) }
        break if stream_identities.empty?

        stream_identities.each do |stream_identity|
          flushed[stream_identity] = true
          tag, time, record = @timeout_map_mutex.synchronize do
            elements = @buffer.fetch(stream_identity, nil)
            next if elements.nil? || elements.empty?

            lines = forced_buffered_lines(elements)
            first_tag, first_time, first_record = elements.first
            [first_tag, first_time, first_record.merge(@key => lines.join(@separator))]
          end
          next unless record

          message = "Flush remaining buffer: #{stream_identity_label(stream_identity)}"
          handle_timeout_error(tag, time, record, message)
          log.info(message)
        end
      end
      @timeout_map_mutex.synchronize { @buffer.clear }
    end

    # The tag and the identity can have incompatible encodings, for example a
    # UTF-8 tag and a binary identity, both with non-ASCII bytes. Joining them
    # must not raise, or the flush would stop before the record is emitted.
    def stream_identity_label(stream_identity)
      stream_identity.join(":")
    rescue Encoding::CompatibilityError
      stream_identity.map(&:b).join(":")
    end

    def handle_timeout_error(tag, time, record, message)
      if @timeout_label
        event_router = event_emitter_router(@timeout_label)
        event_router.emit(tag, time, record)
      else
        router.emit_error_event(tag, time, record, TimeoutError.new(message))
      end
    end

    def record_bytesize(record)
      record.keys.sum(&:bytesize) + record.values.sum { |v| v.to_s.bytesize }
    end
  end
end

class Array
  # Support Ruby 2.3 or earlier
  unless [].respond_to?(:sum)
    def sum
      inject(0) do |memo, value|
        if block_given?
          memo + yield(value)
        else
          memo + value
        end
      end
    end
  end
end
