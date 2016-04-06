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

  class Config < self
    def test_empty
      assert_raise(Fluent::ConfigError, "key parameter is required") do
        create_driver("")
      end
    end

    def test_exclusive
      assert_raise(Fluent::ConfigError, "n_lines and multiline_start_regexp are exclusive") do
        create_driver(<<~CONFIG)
          key message
          n_lines 10
          multiline_start_regexp /^start/
        CONFIG
      end
    end

    def test_either
      assert_raise(Fluent::ConfigError, "Either n_lines or multiline_start_regexp is required") do
        create_driver(<<~CONFIG)
          key message
        CONFIG
      end
    end

    def test_n_lines
      d = create_driver
      assert_equal(:line, d.instance.instance_variable_get(:@mode))
    end

    def test_multiline_start_regexp
      d = create_driver(<<~CONFIG)
        key message
        multiline_start_regexp /^start/
      CONFIG
      assert_equal(:regexp, d.instance.instance_variable_get(:@mode))
    end
  end
end
