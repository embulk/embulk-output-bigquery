Gem::Specification.new do |spec|
  spec.name          = "embulk-output-bigquery"
  spec.version       = "0.7.2"
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

  # the latest version
  spec.add_dependency 'google-apis-storage_v1'
  spec.add_dependency 'google-apis-bigquery_v2'
  spec.add_dependency 'time_with_zone'
  spec.add_dependency 'thwait'
  # activesupport require Ruby >= 2.7.0
  # jruby-9.3.0.0 is MRI 2.6 compatible
  spec.add_dependency 'activesupport', "< 7.0"

  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
