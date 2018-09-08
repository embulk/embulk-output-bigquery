Gem::Specification.new do |spec|
  spec.name          = "embulk-output-bigquery"
  spec.version       = "0.4.9"
  spec.authors       = ["Satoshi Akama", "Naotoshi Seo"]
  spec.summary       = "Google BigQuery output plugin for Embulk"
  spec.description   = "Embulk plugin that insert records to Google BigQuery."
  spec.email         = ["satoshiakama@gmail.com", "sonots@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/embulk/embulk-output-bigquery"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'google-api-client'
  spec.add_dependency 'time_with_zone'

  spec.add_development_dependency 'embulk', ['>= 0.8.2']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
