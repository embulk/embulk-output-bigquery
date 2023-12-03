#!/usr/bin/env ruby

require 'bundler/setup'
require 'test/unit'
require 'test/unit/rr'

# Embulk 0.10.x introduced new bootstrap mechanism.
# https://github.com/embulk/embulk/blob/641f35fec064cca7b1a7314d634a4b64ef8637f1/embulk-ruby/test/vanilla/run-test.rb#L8-L13
static_initializer = Java::org.embulk.EmbulkDependencyClassLoader.staticInitializer().useSelfContainedJarFiles()
static_initializer.java_send :initialize

require 'embulk/java/bootstrap'
require 'embulk'

Embulk.logger = Embulk::Logger.new('/dev/null')

APP_ROOT = File.expand_path('../', __dir__)
EXAMPLE_ROOT = File.expand_path('../example', __dir__)
TEST_ROOT = File.expand_path(File.dirname(__FILE__))
JSON_KEYFILE = File.join(EXAMPLE_ROOT, 'your-project-000.json')
