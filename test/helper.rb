#!/usr/bin/env ruby

require 'test/unit'
require 'test/unit/rr'

# ToDo: https://github.com/embulk/embulk/issues/406
# require 'embulk/java/bootstrap'
# require 'embulk'
# Embulk.setup
require_relative 'fake_embulk'

APP_ROOT = File.expand_path('../', __dir__)
EXAMPLE_ROOT = File.expand_path('../example', __dir__)
TEST_ROOT = File.expand_path(File.dirname(__FILE__))
