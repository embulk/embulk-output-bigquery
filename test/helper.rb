#!/usr/bin/env ruby

require 'bundler/setup'
require 'test/unit'
require 'test/unit/rr'

require 'embulk'
begin
  # Embulk ~> 0.8.x
  Embulk.setup
rescue NotImplementedError, NoMethodError, NameError
  # Embulk ~> 0.9.x
  require 'embulk/java/bootstrap'
end
Embulk.logger = Embulk::Logger.new('/dev/null')

APP_ROOT = File.expand_path('../', __dir__)
EXAMPLE_ROOT = File.expand_path('../example', __dir__)
TEST_ROOT = File.expand_path(File.dirname(__FILE__))
JSON_KEYFILE = File.join(EXAMPLE_ROOT, 'your-project-000.json')
