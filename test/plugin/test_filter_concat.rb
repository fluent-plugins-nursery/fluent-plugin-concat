require "helper"

class FilterConcatTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @time = Fluent::Engine.now
  end

  CONFIG = %[
    key message
    n_lines 3
  ]

  def create_driver(conf = CONFIG, tag = "test")
    Fluent::Test::FilterTestDriver.new(Fluent::ConcatFilter, tag).configure(conf, true)
  end

  def filter(conf, messages, wait: nil)
    d = create_driver(conf)
    yield d if block_given?
    d.run do
      sleep 0.1 # run event loop
      messages.each do |message|
        d.filter(message, @time)
      end
      sleep wait if wait
    end
    filtered = d.filtered_as_array
    filtered.map {|m| m[2] }
  end

  def filter_with_time(conf, messages, wait: nil)
    d = create_driver(conf)
    yield d if block_given?
    d.run do
      sleep 0.1 # run event loop
      messages.each do |message, time|
        d.filter(message, time)
      end
      sleep wait if wait
    end
    d.filtered_as_array
  end

  class Config < self
    def test_empty
      assert_raise(Fluent::ConfigError, "key parameter is required") do
        create_driver("")
      end
    end

    def test_exclusive
      assert_raise(Fluent::ConfigError, "n_lines and multiline_start_regexp are exclusive") do
        create_driver(<<-CONFIG)
          key message
          n_lines 10
          multiline_start_regexp /^start/
        CONFIG
      end
    end

    def test_either
      assert_raise(Fluent::ConfigError, "Either n_lines or multiline_start_regexp is required") do
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
      filtered = filter(config + "flush_interval 1s", messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "start\n  message 1\n  message 2" }
        mock(d.instance.router).emit_error_event("test", anything, errored, anything)
      end
      assert_equal([], filtered)
    end
  end

  class UseFirstTimestamp < self
    def test_filter_true
      messages = [
        [{ "host" => "example.com", "message" => "message 1" }, @time],
        [{ "host" => "example.com", "message" => "message 2" }, @time + 1],
        [{ "host" => "example.com", "message" => "message 3" }, @time + 2],
      ]
      expected = { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" }
      conf = CONFIG + "use_first_timestamp true"
      filtered = filter_with_time(conf, messages)
      assert_equal(@time, filtered[0][1])
      assert_equal(expected, filtered[0][2])
    end

    def test_filter_false
      messages = [
        [{ "host" => "example.com", "message" => "message 1" }, @time],
        [{ "host" => "example.com", "message" => "message 2" }, @time + 1],
        [{ "host" => "example.com", "message" => "message 3" }, @time + 2],
      ]
      expected = { "host" => "example.com", "message" => "message 1\nmessage 2\nmessage 3" }
      conf = CONFIG + "use_first_timestamp false"
      filtered = filter_with_time(conf, messages)
      assert_equal(@time + 2, filtered[0][1])
      assert_equal(expected, filtered[0][2])
    end

    def test_timeout
      config = <<-CONFIG
        key message
        multiline_start_regexp /^start/
        flush_interval 1s
        use_first_timestamp true
      CONFIG
      messages = [
        [{ "container_id" => "1", "message" => "start" }, @time],
        [{ "container_id" => "1", "message" => "  message 1" }, @time],
        [{ "container_id" => "1", "message" => "  message 2" }, @time],
        [{ "container_id" => "1", "message" => "start" }, @time],
        [{ "container_id" => "1", "message" => "  message 3" }, @time + 1],
        [{ "container_id" => "1", "message" => "  message 4" }, @time + 2],
      ]
      filtered = filter_with_time(config, messages, wait: 3) do |d|
        errored = { "container_id" => "1", "message" => "start\n  message 3\n  message 4" }
        mock(d.instance.router).emit_error_event("test", @time, errored, anything)
      end
      expected = { "container_id" => "1", "message" => "start\n  message 1\n  message 2" }
      assert_equal(expected, filtered[0][2])
    end
  end
end
