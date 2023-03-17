Gem::Specification.new do |spec|
  spec.name          = "embulk-output-bigquery"
  spec.version       = "0.6.9"
  spec.authors       = ["Satoshi Akama", "Naotoshi Seo"]
  spec.summary       = "Google BigQuery output plugin for Embulk"
  spec.description   = "Embulk plugin that insert records to Google BigQuery."
  spec.email         = ["satoshiakama@gmail.com", "sonots@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/embulk/embulk-output-bigquery"

  # Exclude example directory which uses symlinks from generating gem.
  # Symlinks do not work properly on the Windows platform without administrator privilege.
  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"] - Dir["example/*" ]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  # TODO
  # signet 0.12.0 and google-api-client 0.33.0 require >= Ruby 2.4.
  # Embulk 0.9 use JRuby 9.1.X.Y and it's compatible with Ruby 2.3.
  # So, force install signet < 0.12 and google-api-client < 0.33.0
  # Also, representable version >= 3.1.0 requires Ruby version >= 2.4
  spec.add_dependency 'signet', '~> 0.7', '< 0.12.0'
  spec.add_dependency 'google-api-client','< 0.33.0'
  spec.add_dependency 'time_with_zone'
  spec.add_dependency "representable", ['~> 3.0.0', '< 3.1']
  # faraday 1.1.0 require >= Ruby 2.4.
  # googleauth 0.9.0 requires faraday ~> 0.12
  spec.add_dependency "faraday", '~> 0.12'

  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
