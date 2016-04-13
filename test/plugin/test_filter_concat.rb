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

  def filter(conf, messages)
    d = create_driver(conf)
    d.run do
      messages.each do |message|
        d.filter(message, @time)
      end
    end
    filtered = d.filtered_as_array
    filtered.map {|m| m[2] }
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
      filtered = filter(config, messages)
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
  end
end
