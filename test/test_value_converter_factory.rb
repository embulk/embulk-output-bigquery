require_relative './helper'
require 'embulk/output/bigquery/value_converter_factory'

module Embulk
  class Output::Bigquery
    class TestValueConverterFactory < Test::Unit::TestCase

      class TestCreateConverters < Test::Unit::TestCase
        def test_create_default_converter
          schema = Schema.new([
            Column.new({index: 0, name: 'boolean', type: :boolean}),
            Column.new({index: 1, name: 'long', type: :long}),
            Column.new({index: 2, name: 'double', type: :double}),
            Column.new({index: 3, name: 'string', type: :string}),
            Column.new({index: 4, name: 'timestamp', type: :timestamp}),
            Column.new({index: 5, name: 'json', type: :json}),
          ])
          converters = ValueConverterFactory.create_converters({}, schema)
          assert_equal schema.size, converters.size
          # Check correct converters are created
          # Proc can not have names, so we have to execute to check...
          assert_equal true, converters[0].call(true)
          assert_equal 1, converters[1].call(1)
          assert_equal 1.1, converters[2].call(1.1)
          assert_equal 'foo', converters[3].call('foo')
          timestamp = Time.parse("2016-02-26 00:00:00.500000 +00:00")
          assert_equal "2016-02-26 00:00:00.500000 +00:00", converters[4].call(timestamp)
          assert_equal %Q[{"foo":"foo"}], converters[5].call({'foo'=>'foo'})
        end

        def test_create_custom_converter
          schema = Schema.new([
            Column.new({index: 0, name: 'boolean', type: :boolean}),
            Column.new({index: 1, name: 'long', type: :long}),
            Column.new({index: 2, name: 'double', type: :double}),
            Column.new({index: 3, name: 'string', type: :string}),
            Column.new({index: 4, name: 'timestamp', type: :timestamp}),
            Column.new({index: 5, name: 'json', type: :json}),
          ])
          task = {
            'column_options' => [
              {'name' => 'boolean',   'type' => 'STRING'},
              {'name' => 'long',      'type' => 'STRING'},
              {'name' => 'double',    'type' => 'STRING'},
              {'name' => 'string',    'type' => 'INTEGER'},
              {'name' => 'timestamp', 'type' => 'INTEGER'},
              {'name' => 'json',      'type' => 'RECORD'},
            ],
          }
          converters = ValueConverterFactory.create_converters(task, schema)
          assert_equal schema.size, converters.size
          # Check correct converters are created
          # Proc can not have names, so we have to execute to check...
          assert_equal 'true', converters[0].call(true)
          assert_equal '1', converters[1].call(1)
          assert_equal '1.1', converters[2].call(1.1)
          assert_equal 1, converters[3].call('1')
          timestamp = Time.parse("2016-02-26 00:00:00.100000 +00:00")
          assert_equal 1456444800, converters[4].call(timestamp)
          assert_equal({'foo'=>'foo'}, converters[5].call({'foo'=>'foo'}))
        end
      end

      class TestBooleanConverter < Test::Unit::TestCase
        SCHEMA_TYPE = :boolean

        def test_boolean
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'BOOLEAN').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal true, converter.call(true)
          assert_equal false, converter.call(false)
        end

        def test_integer
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'INTEGER').create_converter }
        end

        def test_float
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'FLOAT').create_converter }
        end

        def test_string
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'STRING').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "true", converter.call(true)
          assert_equal "false", converter.call(false)
        end

        def test_timestamp
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'TIMESTAMP').create_converter }
        end

        def test_date
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATE').create_converter }
        end

        def test_datetime
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATETIME').create_converter }
        end

        def test_record
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'RECORD').create_converter }
        end
      end

      class TestLongConverter < Test::Unit::TestCase
        SCHEMA_TYPE = :long

        def test_boolean
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'BOOLEAN').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal true, converter.call(1)
          assert_equal false, converter.call(0)
          assert_raise { converter.call(2) }
        end

        def test_integer
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'INTEGER').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1, converter.call(1)
        end

        def test_float
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'FLOAT').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1.0, converter.call(1)
        end

        def test_string
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'STRING').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "1", converter.call(1)
        end

        def test_timestamp
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'TIMESTAMP').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1408452095, converter.call(1408452095)
        end

        def test_date
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATE').create_converter }
        end

        def test_datetime
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATETIME').create_converter }
        end

        def test_record
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'RECORD').create_converter }
        end
      end

      class TestDoubleConverter < Test::Unit::TestCase
        SCHEMA_TYPE = :double

        def test_boolean
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'BOOLEAN').create_converter }
        end

        def test_integer
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'INTEGER').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1, converter.call(1.1)
        end

        def test_float
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'FLOAT').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1.1, converter.call(1.1)
        end

        def test_string
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'STRING').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "1.1", converter.call(1.1)
        end

        def test_timestamp
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'TIMESTAMP').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1408452095.188766, converter.call(1408452095.188766)
        end

        def test_date
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATE').create_converter }
        end

        def test_datetime
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATETIME').create_converter }
        end

        def test_record
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'RECORD').create_converter }
        end
      end

      class TestStringConverter < Test::Unit::TestCase
        SCHEMA_TYPE = :string

        def test_boolean
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'BOOLEAN').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal true, converter.call('true')
          assert_equal false, converter.call('false')
          assert_raise { converter.call('foo') }
        end

        def test_integer
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'INTEGER').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1, converter.call('1')
          assert_raise { converter.call('1.1') }
        end

        def test_float
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'FLOAT').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal 1.1, converter.call('1.1')
          assert_raise { converter.call('foo') }
        end

        def test_string
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'STRING').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "foo", converter.call("foo")
        end

        def test_timestamp
          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'TIMESTAMP',
            timestamp_format: '%Y-%m-%d', timezone: 'Asia/Tokyo'
          ).create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "2016-02-26 00:00:00.000000 +09:00", converter.call("2016-02-26")

          # Users must care of BQ timestamp format by themselves with no timestamp_format
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'TIMESTAMP').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "2016-02-26 00:00:00", converter.call("2016-02-26 00:00:00")
        end

        def test_date
          converter = ValueConverterFactory.new(
              SCHEMA_TYPE, 'DATE',
              timestamp_format: '%Y/%m/%d'
          ).create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "2016-02-26", converter.call("2016/02/26")

          # Users must care of BQ date format by themselves with no timestamp_format
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'DATE').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "2016-02-26", converter.call("2016-02-26")
        end

        def test_datetime
          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'DATETIME',
            timestamp_format: '%Y/%m/%d'
          ).create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "2016-02-26 00:00:00.000000", converter.call("2016/02/26")

          # Users must care of BQ datetime format by themselves with no timestamp_format
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'DATETIME').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "2016-02-26 00:00:00", converter.call("2016-02-26 00:00:00")
        end

        def test_time
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'TIME').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal "00:03:22.000000", converter.call("00:03:22")
          assert_equal "15:22:00.000000", converter.call("3:22 PM")
          assert_equal "03:22:00.000000", converter.call("3:22 AM")
          assert_equal "00:00:00.000000", converter.call("2016-02-26 00:00:00")

           # TimeWithZone doesn't affect any change to the time value
          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'TIME', timezone: 'Asia/Tokyo'
          ).create_converter
          assert_equal "15:00:01.000000", converter.call("15:00:01")

          assert_raise { converter.call('foo') }
        end

        def test_record
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'RECORD').create_converter
          assert_equal({'foo'=>'foo'}, converter.call(%Q[{"foo":"foo"}]))
          assert_raise { converter.call('foo') }
        end
      end

      class TestTimestampConverter < Test::Unit::TestCase
        SCHEMA_TYPE = :timestamp

        def test_boolean
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'BOOLEAN').create_converter }
        end

        def test_integer
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'INTEGER').create_converter
          assert_equal nil, converter.call(nil)
          expected = 1456444800
          assert_equal expected, converter.call(Time.at(expected))
        end

        def test_float
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'FLOAT').create_converter
          assert_equal nil, converter.call(nil)
          expected = 1456444800.500000
          assert_equal expected, converter.call(Time.at(expected))
        end

        def test_string
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'STRING').create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-26 00:00:00.500000 +00:00")
          expected = "2016-02-26 00:00:00.500000"
          assert_equal expected, converter.call(timestamp)

          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'STRING',
            timestamp_format: '%Y-%m-%d', timezone: 'Asia/Tokyo'
          ).create_converter
          timestamp = Time.parse("2016-02-25 15:00:00.500000 +00:00")
          expected = "2016-02-26"
          assert_equal expected, converter.call(timestamp)
        end

        def test_timestamp
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'TIMESTAMP').create_converter
          assert_equal nil, converter.call(nil)
          subject = 1456444800.500000
          expected = "2016-02-26 00:00:00.500000 +00:00"
          assert_equal expected, converter.call(Time.at(subject).utc)
        end

        def test_date
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'DATE').create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-26 00:00:00.500000 +00:00")
          expected = "2016-02-26"
          assert_equal expected, converter.call(timestamp)

          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'DATE', timezone: 'Asia/Tokyo'
          ).create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-25 15:00:00.500000 +00:00")
          expected = "2016-02-26"
          assert_equal expected, converter.call(timestamp)

          assert_raise { converter.call('foo') }
        end

        def test_datetime
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'DATETIME').create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-26 00:00:00.500000 +00:00")
          expected = "2016-02-26 00:00:00.500000"
          assert_equal expected, converter.call(timestamp)

          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'DATETIME', timezone: 'Asia/Tokyo'
          ).create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-25 15:00:00.500000 +00:00")
          expected = "2016-02-26 00:00:00.500000"
          assert_equal expected, converter.call(timestamp)

          assert_raise { converter.call('foo') }
        end

        def test_time
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'TIME').create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-26 00:00:00.500000 +00:00")
          expected = "00:00:00.500000"
          assert_equal expected, converter.call(timestamp)

          converter = ValueConverterFactory.new(
            SCHEMA_TYPE, 'TIME', timezone: 'Asia/Tokyo'
          ).create_converter
          assert_equal nil, converter.call(nil)
          timestamp = Time.parse("2016-02-25 15:00:00.500000 +00:00")
          expected = "00:00:00.500000"
          assert_equal expected, converter.call(timestamp)

          assert_raise { converter.call('foo') }
        end

        def test_record
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'RECORD').create_converter }
        end
      end

      class TestJsonConverter < Test::Unit::TestCase
        SCHEMA_TYPE = :json

        def test_boolean
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'BOOLEAN').create_converter }
        end

        def test_integer
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'INTEGER').create_converter }
        end

        def test_float
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'FLOAT').create_converter }
        end

        def test_string
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'STRING').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal(%Q[{"foo":"foo"}], converter.call({'foo'=>'foo'}))
        end

        def test_timestamp
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'TIMESTAMP').create_converter }
        end

        def test_date
          assert_raise { ValueConverterFactory.new(SCHEMA_TYPE, 'DATE').create_converter }
        end

        def test_record
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'RECORD').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal({'foo'=>'foo'}, converter.call({'foo'=>'foo'}))
        end

        def test_json
          converter = ValueConverterFactory.new(SCHEMA_TYPE, 'JSON').create_converter
          assert_equal nil, converter.call(nil)
          assert_equal({'foo'=>'foo'}, converter.call({'foo'=>'foo'}))
        end
      end

      def test_strict_false
        converter = ValueConverterFactory.new(:string, 'BOOLEAN', strict: false).create_converter
        assert_equal nil, converter.call('foo')

        converter = ValueConverterFactory.new(:string, 'INTEGER', strict: false).create_converter
        assert_equal nil, converter.call('foo')
      end
    end
  end
end
