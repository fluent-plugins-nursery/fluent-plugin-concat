require "helper"
require "fluent/test/driver/filter"
class FilterConcatTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @time = Fluent::Engine.now
  end

  CONFIG = %[
    key message
    n_lines 3
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::ConcatFilter).configure(conf, syntax: :v1)
  end

  def filter(conf, messages, wait: nil)
    d = create_driver(conf)
    yield d if block_given?
    d.run(default_tag: "test") do
      sleep 0.1 # run event loop
      messages.each do |message|
        d.feed(@time, message)
      end
      sleep wait if wait
    end
    d.filtered_records
  end

  def filter_with_time(conf, messages, wait: nil)
    d = create_driver(conf)
    yield d if block_given?
    d.run(default_tag: "test") do
      sleep 0.1 # run event loop
      messages.each do |time, message|
        d.feed(time, message)
      end
      sleep wait if wait
    end
    d.filtered
  end

  class Config < self
    def test_empty
      assert_raise(Fluent::ConfigError, "key parameter is required") do
        create_driver("")
      end
    end

    def test_no_regexp_key
      d = create_driver(<<-CONFIG)
        key message 
        multiline_start_regexp /^start/
      CONFIG
      assert_equal("message", d.instance.instance_variable_get(:@key))
      assert_equal("message", d.instance.instance_variable_get(:@regexp_key))
    end

    def test_regexp_key
      d = create_driver(<<-CONFIG)
        key message 
        regexp_key prefix
        multiline_start_regexp /^start/
      CONFIG
      assert_equal("message", d.instance.instance_variable_get(:@key))
      assert_equal("prefix", d.instance.instance_variable_get(:@regexp_key))
    end

    def test_exclusive
      assert_raise(Fluent::ConfigError, "n_lines and multiline_start_regexp/multiline_end_regexp are exclusive") do
        create_driver(<<-CONFIG)
          key message
          n_lines 10
          multiline_start_regexp /^start/
        CONFIG
      end
    end

    def test_either
      assert_raise(Fluent::ConfigError, "Either n_lines or multiline_start_regexp or multiline_end_regexp is required") do
        create_driver(<<-CONFIG)
          key message
        CONFIG
      end
    end

    def test_n_lines
      d = create_driver
      assert_equal(:line, d.instance.instance_variable_get(:@mode))
    end

    def test_multiline_start_regexp
      d = create_driver(<<-CONFIG)
        key message
        multiline_start_regexp /^start/
      CONFIG
      assert_equal(:regexp, d.instance.instance_variable_get(:@mode))
    end

    def test_multiline_end_regexp
      d = create_driver(<<-CONFIG)
        key message
        multiline_end_regexp /^end/
      CONFIG
      assert_equal(:regexp, d.instance.instance_variable_get(:@mode))
    end
  end

  class Lines < self
    def test_filter
      messages = [
        { "host" => "example.com", "message" => "message 1" },
        { "host" => "example.com", "message" => "message 2" },
        { "host" => "example.com", "message" => "message 3" },
      ]
      expected = [
        { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" }
      ]
      filtered = filter(CONFIG, messages)
      assert_equal(expected, filtered)
    end

    def test_filter_excess
      messages = [
        { "host" => "example.com", "message" => "message 1" },
        { "host" => "example.com", "message" => "message 2" },
        { "host" => "example.com", "message" => "message 3" },
        { "host" => "example.com", "message" => "message 4" },
      ]
      expected = [
        { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" }
      ]
      filtered = filter(CONFIG, messages)
      assert_equal(expected, filtered)
    end

    def test_filter_2_groups
      messages = [
        { "host" => "example.com", "message" => "message 1" },
        { "host" => "example.com", "message" => "message 2" },
        { "host" => "example.com", "message" => "message 3" },
        { "host" => "example.com", "message" => "message 4" },
        { "host" => "example.com", "message" => "message 5" },
        { "host" => "example.com", "message" => "message 6" },
      ]
      expected = [
        { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" },
        { "host" => "example.com", "message" => "message 4\nmessage 5\nmessage 6" },
      ]
      filtered = filter(CONFIG, messages)
      assert_equal(expected, filtered)
    end
    
    def test_missing_keys
      messages = [
        { "host" => "example.com", "message" => "message 1" },
        { "host" => "example.com", "message" => "message 2" },
        { "host" => "example.com", "message" => "message 3" },
        { "host" => "example.com", "message" => "message 4" },
        { "host" => "example.com", "message" => "message 5" },
        { "host" => "example.com", "message" => "message 6" },
        { "host" => "example.com", "somekey" => "message 7" },
      ]
      expected = [
        { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" },
        { "host" => "example.com", "message" => "message 4\nmessage 5\nmessage 6" },
        { "host" => "example.com", "somekey" => "message 7" },
      ]
      filtered = filter(CONFIG, messages)
      assert_equal(expected, filtered)
    end

    def test_stream_identity
      messages = [
        { "container_id" => "1", "message" => "message 1" },
        { "container_id" => "2", "message" => "message 2" },
        { "container_id" => "1", "message" => "message 3" },
        { "container_id" => "2", "message" => "message 4" },
        { "container_id" => "1", "message" => "message 5" },
        { "container_id" => "2", "message" => "message 6" },
      ]
      expected = [
        { "container_id" => "1", "message" => "message 1\nmessage 3\nmessage 5" },
        { "container_id" => "2", "message" => "message 2\nmessage 4\nmessage 6" },
      ]
      filtered = filter(CONFIG + "stream_identity_key container_id", messages)
      assert_equal(expected, filtered)
    end

    def test_timeout
      messages = [
        { "container_id" => "1", "message" => "message 1" },
        { "container_id" => "1", "message" => "message 2" },
      ]
      filtered = filter(CONFIG + "flush_interval 2s", messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "message 1\nmessage 2" }
        mock(d.instance.router).emit_error_event("test", anything, errored, anything)
      end
      assert_equal([], filtered)
    end

    def test_timeout_with_timeout_label
      messages = [
        { "container_id" => "1", "message" => "message 1" },
        { "container_id" => "1", "message" => "message 2" },
      ]
      filtered = filter(CONFIG + "flush_interval 2s\ntimeout_label @TIMEOUT", messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "message 1\nmessage 2" }
        event_router = mock(Object.new).emit("test", anything, errored)
        mock(Fluent::Test::Driver::TestEventRouter).new(anything) { event_router }
      end
      assert_equal([], filtered)
    end

    def test_no_timeout
      messages = [
        { "container_id" => "1", "message" => "message 1" },
        { "container_id" => "1", "message" => "message 2" },
        { "container_id" => "1", "message" => "message 3" },
      ]
      filtered = filter(CONFIG + "flush_interval 30s", messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "message 1\nmessage 2\nmessage 3" }
        mock(d.instance.router).emit_error_event("test", anything, errored, anything).times(0)
      end
      expected = [
        { "container_id" => "1", "message" => "message 1\nmessage 2\nmessage 3" },
      ]
      assert_equal(expected, filtered)
    end
  end

  class Regexp < self
    def test_filter
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
      CONFIG
      messages = [
        { "host" => "example.com", "message" => "start" },
        { "host" => "example.com", "message" => "  message 1" },
        { "host" => "example.com", "message" => "  message 2" },
        { "host" => "example.com", "message" => "start" },
        { "host" => "example.com", "message" => "  message 3" },
        { "host" => "example.com", "message" => "  message 4" },
        { "host" => "example.com", "message" => "start" },
      ]
      expected = [
        { "host" => "example.com", "message" => "start\n  message 1\n  message 2" },
        { "host" => "example.com", "message" => "start\n  message 3\n  message 4" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    def test_regexp_key
      config = <<-CONFIG
        key message
        regexp_key split
        multiline_end_regexp /^full$/
      CONFIG
      messages = [
        { "host" => "example.com", "split" => "partial", "message" => "message 1" },
        { "host" => "example.com", "split" => "full", "message" => "message 2" },
        { "host" => "example.com", "message" => "message 3" },
        { "host" => "example.com", "split" => "full", "message" => "message 4" },
        { "host" => "example.com", "split" => "partial", "message" => "message 5" },
        { "host" => "example.com", "split" => "full", "message" => "message 6" },
      ]
      expected = [
        { "host" => "example.com", "split" => "partial", "message" => "message 1\nmessage 2" },
        { "host" => "example.com", "message" => "message 3" },
        { "host" => "example.com", "split" => "full", "message" => "message 4" },
        { "host" => "example.com", "split" => "partial", "message" => "message 5\nmessage 6" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    def test_stream_identity
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_start_regexp /^start/
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "2", "message" => "start" },
        { "container_id" => "2", "message" => "  message 3" },
        { "container_id" => "2", "message" => "  message 4" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "2", "message" => "  message 5" },
        { "container_id" => "2", "message" => "start" },
      ]
      expected = [
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2" },
        { "container_id" => "2", "message" => "start\n  message 3\n  message 4\n  message 5" },
      ]
      filtered = filter(config, messages) do |d|
        errored1 = { "container_id" => "1", "message" => "start" }
        errored2 = { "container_id" => "2", "message" => "start" }
        router = d.instance.router
        mock(router).emit_error_event("test", anything, errored1, anything)
        mock(router).emit_error_event("test", anything, errored2, anything)
      end
      assert_equal(expected, filtered)
    end

    def test_multiline_end_regexp
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_start_regexp /^start/
        multiline_end_regexp /^end/
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "2", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "2", "message" => "  message 3" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "2", "message" => "  message 4" },
        { "container_id" => "1", "message" => "end" },
        { "container_id" => "2", "message" => "end" },
      ]
      expected = [
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2\nend" },
        { "container_id" => "2", "message" => "start\n  message 3\n  message 4\nend" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    def test_multiline_with_single_line_logs
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_start_regexp /^start/
        multiline_end_regexp /^end/
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "single1" },
        { "container_id" => "2", "message" => "single2" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "2", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "2", "message" => "  message 3" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "2", "message" => "  message 4" },
        { "container_id" => "1", "message" => "end" },
        { "container_id" => "2", "message" => "end" },
        { "container_id" => "1", "message" => "single3" },
        { "container_id" => "2", "message" => "single4" },
      ]
      expected = [
        { "container_id" => "1", "message" => "single1" },
        { "container_id" => "2", "message" => "single2" },
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2\nend" },
        { "container_id" => "2", "message" => "start\n  message 3\n  message 4\nend" },
        { "container_id" => "1", "message" => "single3" },
        { "container_id" => "2", "message" => "single4" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    def test_multiline_start_end_regexp
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_start_regexp /^start/
        multiline_end_regexp /end$/
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start message end" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => " message1" },
        { "container_id" => "1", "message" => " message2" },
        { "container_id" => "1", "message" => "end" },
      ]
      expected = [
        { "container_id" => "1", "message" => "start message end" },
        { "container_id" => "1", "message" => "start\n message1\n message2\nend" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    def test_multiline_end_only_regexp
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_end_regexp /\\n$/
      CONFIG
      messages = [
          { "host" => "example.com", "message" => "{\"key1\": \"value1\",\"key2\": \"value2\"}\n" },
          { "host" => "example.com", "message" => "{\"key3\": \"value3\",\"key4\": \"value4\"," },
          { "host" => "example.com", "message" => "\"key5\": \"value5\",\"key6\": \"value6\"," },
          { "host" => "example.com", "message" => "\"key7\": \"value7\",\"key8\": \"value8\"}\n" },
          { "host" => "example.com", "message" => "{\"key9\": \"value9\",\"key0\": \"value0\"," },
          { "host" => "example.com", "message" => "\"key1\": \"value1\",\"key2\": \"value2\"}\n" },
      ]
      expected = [
          { "host" => "example.com","message" => "{\"key1\": \"value1\",\"key2\": \"value2\"}\n" },
          { "host" => "example.com","message" => "{\"key3\": \"value3\",\"key4\": \"value4\",\n\"key5\": \"value5\",\"key6\": \"value6\",\n\"key7\": \"value7\",\"key8\": \"value8\"}\n" },
          { "host" => "example.com","message" => "{\"key9\": \"value9\",\"key0\": \"value0\",\n\"key1\": \"value1\",\"key2\": \"value2\"}\n" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    # https://github.com/okkez/fluent-plugin-concat/issues/14
    def test_multiline_start_end_regexp_github14
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_start_regexp /^start/
        multiline_end_regexp /end$/
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start message1 end" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => " message3" },
        { "container_id" => "1", "message" => "start message2 end" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => " message4" },
        { "container_id" => "1", "message" => "end" },
      ]
      expected = [
        { "container_id" => "1", "message" => "start message1 end" },
        { "container_id" => "1", "message" => "start\n message3" },
        { "container_id" => "1", "message" => "start message2 end" },
        { "container_id" => "1", "message" => "start\n message4\nend" },
      ]
      filtered = filter(config, messages)
      assert_equal(expected, filtered)
    end

    def test_timeout
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
        flush_interval 1s
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "1", "message" => "  message 2" },
      ]
      filtered = filter(config, messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "start\n  message 1\n  message 2" }
        mock(d.instance.router).emit_error_event("test", anything, errored, anything)
      end
      assert_equal([], filtered)
    end

    def test_continuous_line
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
        continuous_line_regexp /^ /
        flush_interval 1s
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "1", "message" => "single line message 1" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 3" },
        { "container_id" => "1", "message" => "  message 4" },
        { "container_id" => "1", "message" => "single line message 2" },
      ]
      filtered = filter(config, messages, wait: 3)
      expected = [
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2" },
        { "container_id" => "1", "message" => "single line message 1" },
        { "container_id" => "1", "message" => "start\n  message 3\n  message 4" },
        { "container_id" => "1", "message" => "single line message 2" },
      ]
      assert_equal(expected, filtered)
    end

    def test_missing_keys
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
        continuous_line_regexp /^ /
        flush_interval 1s
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "1", "message" => "single line message 1" },
        { "container_id" => "2", "nomessage" => "This is not message" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 3" },
        { "container_id" => "1", "message" => "  message 4" },
        { "container_id" => "1", "message" => "single line message 2" },
      ]
      filtered = filter(config, messages, wait: 3)
      expected = [
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2" },
        { "container_id" => "1", "message" => "single line message 1" },
        { "container_id" => "2", "nomessage" => "This is not message" },
        { "container_id" => "1", "message" => "start\n  message 3\n  message 4" },
        { "container_id" => "1", "message" => "single line message 2" },
      ]
      assert_equal(expected, filtered)
    end

    def test_value_is_nil
      config = <<-CONFIG
        key message
        stream_identity_key container_id
        multiline_start_regexp /^start/
        continuous_line_regexp /^ /
        flush_interval 1s
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "1", "message" => nil },
        { "container_id" => "1", "message" => "single line message 1" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 3" },
        { "container_id" => "1", "message" => "  message 4" },
        { "container_id" => "1", "message" => "single line message 2" },
      ]
      filtered = filter(config, messages, wait: 3)
      expected = [
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2" },
        { "container_id" => "1", "message" => nil },
        { "container_id" => "1", "message" => "single line message 1" },
        { "container_id" => "1", "message" => "start\n  message 3\n  message 4" },
        { "container_id" => "1", "message" => "single line message 2" },
      ]
      assert_equal(expected, filtered)
    end
  end

  class UseFirstTimestamp < self
    def test_filter_true
      messages = [
        [@time, { "host" => "example.com", "message" => "message 1" }],
        [@time + 1, { "host" => "example.com", "message" => "message 2" }],
        [@time + 2, { "host" => "example.com", "message" => "message 3" }],
      ]
      expected = [
        [@time, { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" }]
      ]
      conf = CONFIG + "use_first_timestamp true"
      filtered = filter_with_time(conf, messages)
      assert_equal(expected, filtered)
    end

    def test_filter_false
      messages = [
        [@time, { "host" => "example.com", "message" => "message 1" }],
        [@time + 1, { "host" => "example.com", "message" => "message 2" }],
        [@time + 2, { "host" => "example.com", "message" => "message 3" }],
      ]
      expected = [
        [@time + 2, { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" }]
      ]
      conf = CONFIG + "use_first_timestamp false"
      filtered = filter_with_time(conf, messages)
      assert_equal(expected, filtered)
    end

    def test_timeout
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
        flush_interval 1s
        use_first_timestamp true
      CONFIG
      messages = [
        [@time, { "container_id" => "1", "message" => "start" }],
        [@time, { "container_id" => "1", "message" => "  message 1" }],
        [@time, { "container_id" => "1", "message" => "  message 2" }],
        [@time, { "container_id" => "1", "message" => "start" }],
        [@time + 1, { "container_id" => "1", "message" => "  message 3" }],
        [@time + 2, { "container_id" => "1", "message" => "  message 4" }],
      ]
      filtered = filter_with_time(config, messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "start\n  message 3\n  message 4" }
        mock(d.instance.router).emit_error_event("test", @time, errored, anything)
      end
      expected = [
        [@time, { "container_id" => "1", "message" => "start\n  message 1\n  message 2" }]
      ]
      assert_equal(expected, filtered)
    end

    def test_disable_timeout
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
        flush_interval 0s
        use_first_timestamp true
      CONFIG
      messages = [
        [@time, { "container_id" => "1", "message" => "start" }],
        [@time, { "container_id" => "1", "message" => "  message 1" }],
        [@time, { "container_id" => "1", "message" => "  message 2" }],
        [@time, { "container_id" => "1", "message" => "start" }],
      ]
      filtered = filter_with_time(config, messages, wait: 3) do |d|
        mock(d.instance).flush_timeout_buffer.at_most(0)
        errored = { "container_id" => "1", "message" => "start" }
        mock(d.instance.router).emit_error_event("test", @time, errored, anything)
      end
      expected = [
        [@time, { "container_id" => "1", "message" => "start\n  message 1\n  message 2" }]
      ]
      assert_equal(expected, filtered)
    end
  end

  sub_test_case "raise exception in on_timer" do
    # See also https://github.com/fluent/fluentd/issues/1946
    test "failed to flush timeout buffer" do
      config = <<-CONFIG
        key message
        flush_interval 1s
        multiline_start_regexp /^start/
      CONFIG
      messages = [
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 1" },
        { "container_id" => "1", "message" => "  message 2" },
        { "container_id" => "1", "message" => "start" },
        { "container_id" => "1", "message" => "  message 3" },
        { "container_id" => "1", "message" => "  message 4" },
        { "container_id" => "1", "message" => "start" },
      ]
      logs = nil
      filtered = filter(config, messages, wait: 3) do |d|
        mock(d.instance).flush_timeout_buffer.times(3) { raise StandardError, "timeout" }
        logs = d.logs
      end
      expected = [
        { "container_id" => "1", "message" => "start\n  message 1\n  message 2" },
        { "container_id" => "1", "message" => "start\n  message 3\n  message 4" }
      ]
      expected_logs = [
        "[error]: failed to flush timeout buffer error_class=StandardError error=\"timeout\"",
        "[error]: failed to flush timeout buffer error_class=StandardError error=\"timeout\"",
        "[error]: failed to flush timeout buffer error_class=StandardError error=\"timeout\"",
        "[info]: Flush remaining buffer: test:default"
      ]
      log_messages = logs.map do |line|
        line.chomp.gsub(/.+? (\[(?:error|info)\].+)/) {|m| $1 }
      end
      assert_equal(expected_logs, log_messages)
      assert_equal(expected, filtered)
    end
  end
end
