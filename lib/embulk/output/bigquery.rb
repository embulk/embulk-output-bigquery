Embulk::JavaPlugin.register_output(
  "bigquery", "org.embulk.output.BigqueryOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
