require_relative './helper'

# 1. Prepare /tmp/your-project-000.json
# 2. embulk bundle
# 3. CONNECT=1 bundle exec ruby test/test_example.rb

if ENV['CONNECT']
  class TestExample < Test::Unit::TestCase
    def embulk_path
      if File.exist?("#{ENV['PATH']}/.embulk/bin/embulk")
        "#{ENV['PATH']}/.embulk/bin/embulk"
      elsif File.exist?("/usr/local/bin/embulk")
        "/usr/local/bin/embulk"
      else
        "embulk"
      end
    end

    files = Dir.glob("#{APP_ROOT}/example/config_*.yml").sort
    files = files.reject {|file| File.symlink?(file) }
    # files.shift
    files.each do |config_path|
      define_method(:"test_#{File.basename(config_path, ".yml")}") do
        success = Bundler.with_clean_env do
          cmd = "#{embulk_path} run -X page_size=1 -b . -l trace #{config_path}"
          puts "=" * 64
          puts cmd
          system(cmd)
        end
        assert_true success
      end
    end
  end
end
