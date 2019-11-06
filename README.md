# embulk-output-bigquery

[![Build Status](https://secure.travis-ci.org/embulk/embulk-output-bigquery.png?branch=master)](http://travis-ci.org/embulk/embulk-output-bigquery)

[Embulk](https://github.com/embulk/embulk/) output plugin to load/insert data into [Google BigQuery](https://cloud.google.com/bigquery/) using [direct insert](https://cloud.google.com/bigquery/loading-data-into-bigquery#loaddatapostrequest)

## Overview

load data into Google BigQuery as batch jobs for big amount of data
https://developers.google.com/bigquery/loading-data-into-bigquery

* **Plugin type**: output
* **Resume supported**: no
* **Cleanup supported**: no
* **Dynamic table creating**: yes

### NOT IMPLEMENTED
* insert data over streaming inserts
  * for continuous real-time insertions
  * Please use other product, like [fluent-plugin-bigquery](https://github.com/kaizenplatform/fluent-plugin-bigquery)
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases

Current version of this plugin supports Google API with Service Account Authentication, but does not support
OAuth flow for installed applications.

## Configuration

#### Original options

| name                                 | type        | required?  | default                  | description            |
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  mode                                | string      | optional   | "append"                 | See [Mode](#mode)      |
|  auth_method                         | string      | optional   | "application\_default"   | See [Authentication](#authentication) |
|  json_keyfile                        | string      | optional   |                          | keyfile path or `content` |
|  project                             | string      | required unless service\_account's `json_keyfile` is given. | | project\_id |
|  dataset                             | string      | required   |                          | dataset |
|  location                            | string      | optional   | nil                      | geographic location of dataset. See [Location](#location) |
|  table                               | string      | required   |                          | table name, or table name with a partition decorator such as `table_name$20160929`|
|  auto_create_dataset                 | boolean     | optional   | false                    | automatically create dataset |
|  auto_create_table                   | boolean     | optional   | true                     | `false` is available only for `append_direct` mode. Other modes require `true`. See [Dynamic Table Creating](#dynamic-table-creating) and [Time Partitioning](#time-partitioning) |
|  schema_file                         | string      | optional   |                          | /path/to/schema.json |
|  template_table                      | string      | optional   |                          | template table name. See [Dynamic Table Creating](#dynamic-table-creating) |
|  job_status_max_polling_time         | int         | optional   | 3600 sec                 | Max job status polling time |
|  job_status_polling_interval         | int         | optional   | 10 sec                   | Job status polling interval |
|  is_skip_job_result_check            | boolean     | optional   | false                    | Skip waiting Load job finishes. Available for append, or delete_in_advance mode |
|  with_rehearsal                      | boolean     | optional   | false                    | Load `rehearsal_counts` records as a rehearsal. Rehearsal loads into REHEARSAL temporary table, and delete finally. You may use this option to investigate data errors as early stage as possible |
|  rehearsal_counts                    | integer     | optional   | 1000                     | Specify number of records to load in a rehearsal |
|  abort_on_error                      | boolean     | optional   | true if max_bad_records is 0, otherwise false | Raise an error if number of input rows and number of output rows does not match |
|  column_options                      | hash        | optional   |                          | See [Column Options](#column-options) |
|  default_timezone                    | string      | optional   | UTC                      | |
|  default_timestamp_format            | string      | optional   | %Y-%m-%d %H:%M:%S.%6N    | |
|  payload_column                      | string      | optional   | nil                      | See [Formatter Performance Issue](#formatter-performance-issue) |
|  payload_column_index                | integer     | optional   | nil                      | See [Formatter Performance Issue](#formatter-performance-issue) |
|  gcs_bucket                          | string      | optional   | nil                      | See [GCS Bucket](#gcs-bucket) |
|  auto_create_gcs_bucket              | boolean     | optional   | false                    | See [GCS Bucket](#gcs-bucket) |
|  progress_log_interval               | float       | optional   | nil (Disabled)           | Progress log interval. The progress log is disabled by nil (default). NOTE: This option may be removed in a future because a filter plugin can achieve the same goal |

Client or request options

| name                                 | type        | required?  | default                  | description            |
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  open_timeout_sec                    | integer     | optional   | 300                      | Seconds to wait for the connection to open |
|  timeout_sec                         | integer     | optional   | 300                      | Seconds to wait for one block to be read (google-api-ruby-client < v0.11.0) |
|  send_timeout_sec                    | integer     | optional   | 300                      | Seconds to wait to send a request (google-api-ruby-client >= v0.11.0) |
|  read_timeout_sec                    | integer     | optional   | 300                      | Seconds to wait to read a response (google-api-ruby-client >= v0.11.0) |
|  retries                             | integer     | optional   | 5                        | Number of retries |
|  application_name                    | string      | optional   | "Embulk BigQuery plugin" | User-Agent |
|  sdk_log_level                       | string      | optional   | nil (WARN)               | Log level of google api client library |

Options for intermediate local files

| name                                 | type        | required?  | default                  | description            |
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  path_prefix                         | string      | optional   |                          | Path prefix of local files such as "/tmp/prefix_". Default randomly generates with [tempfile](http://ruby-doc.org/stdlib-2.2.3/libdoc/tempfile/rdoc/Tempfile.html) |
|  sequence_format                     | string      | optional   | .%d.%d                   | Sequence format for pid, thread id |
|  file_ext                            | string      | optional   |                          | The file extension of local files such as ".csv.gz" ".json.gz". Default automatically generates from `source_format` and `compression`|
|  skip_file_generation                | boolean     | optional   |                          | Load already generated local files into BigQuery if available. Specify correct path_prefix and file_ext. |
|  delete_from_local_when_job_end      | boolean     | optional   | true                     | If set to true, delete generate local files when job is end |
|  compression                         | string      | optional   | "NONE"                   | Compression of local files (`GZIP` or `NONE`) |

`source_format` is also used to determine formatter (csv or jsonl).

#### Same options of bq command-line tools or BigQuery job's property

Following options are same as [bq command-line tools](https://cloud.google.com/bigquery/bq-command-line-tool#creatingtablefromfile) or BigQuery [job's property](https://cloud.google.com/bigquery/docs/reference/v2/jobs#resource).

| name                              | type     | required? | default | description            |
|:----------------------------------|:---------|:----------|:--------|:-----------------------|
|  source_format                    | string   | required  | "CSV"   |   File type (`NEWLINE_DELIMITED_JSON` or `CSV`) |
|  max_bad_records                  | int      | optional  | 0       | |
|  field_delimiter                  | char     | optional  | ","     | |
|  encoding                         | string   | optional  | "UTF-8" | `UTF-8` or `ISO-8859-1` |
|  ignore_unknown_values            | boolean  | optional  | false   | |
|  allow_quoted_newlines            | boolean  | optional  | false   | Set true, if data contains newline characters. It may cause slow procsssing |
|  time_partitioning                | hash     | optional  | `{"type":"DAY"}` if `table` parameter has a partition decorator, otherwise nil | See [Time Partitioning](#time-partitioning) |
|  time_partitioning.type           | string   | required  | nil     | The only type supported is DAY, which will generate one partition per day based on data loading time. |
|  time_partitioning.expiration_ms  | int      | optional  | nil     | Number of milliseconds for which to keep the storage for a partition. |
|  time_partitioning.field          | string   | optional  | nil     | `DATE` or `TIMESTAMP` column used for partitioning |
|  clustering                       | hash     | optional  | nil     | Currently, clustering is supported for partitioned tables, so must be used with `time_partitioning` option. See [clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables) |
|  clustering.fields                | array    | required  | nil     | One or more fields on which data should be clustered. The order of the specified columns determines the sort order of the data. |
|  schema_update_options            | array    | optional  | nil     | (Experimental) List of `ALLOW_FIELD_ADDITION` or `ALLOW_FIELD_RELAXATION` or both. See [jobs#configuration.load.schemaUpdateOptions](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schemaUpdateOptions). NOTE for the current status: `schema_update_options` does not work for `copy` job, that is, is not effective for most of modes such as `append`, `replace` and `replace_backup`. `delete_in_advance` deletes origin table so does not need to update schema. Only `append_direct` can utilize schema update. |

### Example

```yaml
out:
  type: bigquery
  mode: append
  auth_method: service_account
  json_keyfile: /path/to/json_keyfile.json
  project: your-project-000
  dataset: your_dataset_name
  table: your_table_name
  compression: GZIP
  source_format: NEWLINE_DELIMITED_JSON
```

### Location

The geographic location of the dataset. Required except for US and EU.

GCS bucket should be in same region when you use `gcs_bucket`.

See also [Dataset Locations | BigQuery | Google Cloud](https://cloud.google.com/bigquery/docs/dataset-locations)

### Mode

5 modes are provided.

##### append

1. Load to temporary table (Create and WRITE_APPEND in parallel)
2. Copy temporary table to destination table (or partition). (WRITE_APPEND)

##### append_direct

1. Insert data into existing table (or partition) directly. (WRITE_APPEND in parallel)

This is not transactional, i.e., if fails, the target table could have some rows inserted.

##### replace

1. Load to temporary table (Create and WRITE_APPEND in parallel)
2. Copy temporary table to destination table (or partition). (WRITE_TRUNCATE)

```is_skip_job_result_check``` must be false when replace mode

NOTE: BigQuery does not support replacing (actually, copying into) a non-partitioned table with a paritioned table atomically. You must once delete the non-partitioned table, otherwise, you get `Incompatible table partitioning specification when copying to the column partitioned table` error.

##### replace_backup

1. Load to temporary table (Create and WRITE_APPEND in parallel)
2. Copy destination table (or partition) to backup table (or partition). (dataset_old, table_old)
3. Copy temporary table to destination table (or partition). (WRITE_TRUNCATE)

```is_skip_job_result_check``` must be false when replace_backup mode.

##### delete_in_advance

1. Delete destination table (or partition), if it exists.
2. Load to destination table (or partition).

### Authentication

There are four authentication methods

1. `service_account` (or `json_key` for backward compatibility)
1. `authorized_user`
1. `compute_engine`
1. `application_default`

#### service\_account (or json\_key)

Use GCP service account credentials.
You first need to create a service account, download its json key and deploy the key with embulk.

```yaml
out:
  type: bigquery
  auth_method: service_account
  json_keyfile: /path/to/json_keyfile.json
```

You can also embed contents of `json_keyfile` at config.yml.

```yaml
out:
  type: bigquery
  auth_method: service_account
  json_keyfile:
    content: |
      {
          "private_key_id": "123456789",
          "private_key": "-----BEGIN PRIVATE KEY-----\nABCDEF",
          "client_email": "..."
      }
```

#### authorized\_user

Use Google user credentials.
You can get your credentials at `~/.config/gcloud/application_default_credentials.json` by running `gcloud auth login`.

```yaml
out:
  type: bigquery
  auth_method: authorized_user
  json_keyfile: /path/to/credentials.json
```

You can also embed contents of `json_keyfile` at config.yml.

```yaml
out:
  type: bigquery
  auth_method: authorized_user
  json_keyfile:
    content: |
      {
        "client_id":"xxxxxxxxxxx.apps.googleusercontent.com",
        "client_secret":"xxxxxxxxxxx",
        "refresh_token":"xxxxxxxxxxx",
        "type":"authorized_user"
      }
```

#### compute\_engine

On the other hand, you don't need to explicitly create a service account for embulk when you
run embulk in Google Compute Engine. In this third authentication method, you need to
add the API scope "https://www.googleapis.com/auth/bigquery" to the scope list of your
Compute Engine VM instance, then you can configure embulk like this.

```yaml
out:
  type: bigquery
  auth_method: compute_engine
```

#### application\_default

Use Application Default Credentials (ADC).  ADC is a strategy to locate Google Cloud Service Account credentials.

1. ADC checks to see if the environment variable `GOOGLE_APPLICATION_CREDENTIALS` is set. If the variable is set, ADC uses the service account file that the variable points to.
2. ADC checks to see if `~/.config/gcloud/application_default_credentials.json` is located. This file is created by running `gcloud auth application-default login`.
3. Use the default service account for credentials if the application running on Compute Engine, App Engine, Kubernetes Engine, Cloud Functions or Cloud Run.

See https://cloud.google.com/docs/authentication/production for details.

```yaml
out:
  type: bigquery
  auth_method: application_default
```

### Table id formatting

`table` and option accept [Time#strftime](http://ruby-doc.org/core-1.9.3/Time.html#method-i-strftime)
format to construct table ids.
Table ids are formatted at runtime
using the local time of the embulk server.

For example, with the configuration below,
data is inserted into tables `table_20150503`, `table_20150504` and so on.

```yaml
out:
  type: bigquery
  table: table_%Y%m%d
```

### Dynamic table creating

There are 3 ways to set schema.

#### Set schema.json

Please set file path of schema.json.

```yaml
out:
  type: bigquery
  auto_create_table: true
  table: table_%Y%m%d
  schema_file: /path/to/schema.json
```

#### Set template_table in dataset

Plugin will try to read schema from existing table and use it as schema template.

```yaml
out:
  type: bigquery
  auto_create_table: true
  table: table_%Y%m%d
  template_table: existing_table_name
```

#### Guess from Embulk Schema

Plugin will try to guess BigQuery schema from Embulk schema.  It is also configurable with `column_options`. See [Column Options](#column-options).

### Column Options

Column options are used to aid guessing BigQuery schema, or to define conversion of values:

- **column_options**: advanced: an array of options for columns
  - **name**: column name
  - **type**: BigQuery type such as `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP`, `DATETIME`, `DATE`, and `RECORD`. See belows for supported conversion type.
    - boolean:   `BOOLEAN`, `STRING` (default: `BOOLEAN`)
    - long:      `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP` (default: `INTEGER`)
    - double:    `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP` (default: `FLOAT`)
    - string:    `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP`, `DATETIME`, `DATE`, `RECORD` (default: `STRING`)
    - timestamp: `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP`, `DATETIME`, `DATE` (default: `TIMESTAMP`)
    - json:      `STRING`,  `RECORD` (default: `STRING`)
  - **mode**: BigQuery mode such as `NULLABLE`, `REQUIRED`, and `REPEATED` (string, default: `NULLABLE`)
  - **fields**: Describes the nested schema fields if the type property is set to RECORD. Please note that this is **required** for `RECORD` column.
  - **timestamp_format**: timestamp format to convert into/from `timestamp` (string, default is `default_timestamp_format`)
  - **timezone**: timezone to convert into/from `timestamp`, `date` (string, default is `default_timezone`).
- **default_timestamp_format**: default timestamp format for column_options (string, default is "%Y-%m-%d %H:%M:%S.%6N")
- **default_timezone**: default timezone for column_options (string, default is "UTC")

Example)

```yaml
out:
  type: bigquery
  auto_create_table: true
  column_options:
    - {name: date, type: STRING, timestamp_format: %Y-%m-%d, timezone: "Asia/Tokyo"}
    - name: json_column
      type: RECORD
      fields:
        - {name: key1, type: STRING}
        - {name: key2, type: STRING}
```

NOTE: Type conversion is done in this jruby plugin, and could be slow. See [Formatter Performance Issue](#formatter-performance-issue) to improve the performance.

### Formatter Performance Issue

embulk-output-bigquery supports formatting records into CSV or JSON (and also formatting timestamp column).
However, this plugin is written in jruby, and jruby plugins are slower than java plugins generally.

Therefore, it is recommended to format records with filter plugins written in Java such as [embulk-filter-to_json](https://github.com/civitaspo/embulk-filter-to_json) as:

```yaml
filters:
  - type: to_json
    column: {name: payload, type: string}
    default_format: "%Y-%m-%d %H:%M:%S.%6N"
out:
  type: bigquery
  payload_column_index: 0 # or, payload_column: payload
```

Furtheremore, if your files are originally jsonl or csv files, you can even skip a parser with [embulk-parser-none](https://github.com/sonots/embulk-parser-none) as:

```yaml
in:
  type: file
  path_prefix: example/example.jsonl
  parser:
    type: none
    column_name: payload
out:
  type: bigquery
  payload_column_index: 0 # or, payload_column: payload
```

### GCS Bucket

This is useful to reduce number of consumed jobs, which is limited by [100,000 jobs per project per day](https://cloud.google.com/bigquery/quotas#load_jobs).

This plugin originally loads local files into BigQuery in parallel, that is, consumes a number of jobs, say 24 jobs on 24 CPU core machine for example (this depends on embulk parameters such as `min_output_tasks` and `max_threads`).

BigQuery supports loading multiple files from GCS with one job, therefore, uploading local files to GCS in parallel and then loading from GCS into BigQuery reduces number of consumed jobs to 1.

Using `gcs_bucket` option, such strategy is enabled. You may also use `auto_create_gcs_bucket` to create the specified GCS bucket automatically.

```yaml
out:
  type: bigquery
  gcs_bucket: bucket_name
  auto_create_gcs_bucket: true
```

ToDo: Use https://cloud.google.com/storage/docs/streaming if google-api-ruby-client supports streaming transfers into GCS.

### Time Partitioning

From 0.4.0, embulk-output-bigquery supports to load into partitioned table.
See also [Creating and Updating Date-Partitioned Tables](https://cloud.google.com/bigquery/docs/creating-partitioned-tables).

To load into a partition, specify `table` parameter with a partition decorator as:

```yaml
out:
  type: bigquery
  table: table_name$20160929
```

You may configure `time_partitioning` parameter together as:

```yaml
out:
  type: bigquery
  table: table_name$20160929
  time_partitioning:
    type: DAY
    expiration_ms: 259200000
```

You can also create column-based partitioning table as:

```yaml
out:
  type: bigquery
  mode: replace
  table: table_name
  time_partitioning:
    type: DAY
    field: timestamp
```

Note the `time_partitioning.field` should be top-level `DATE` or `TIMESTAMP`.

Use [Tables: patch](https://cloud.google.com/bigquery/docs/reference/v2/tables/patch) API to update the schema of the partitioned table, embulk-output-bigquery itself does not support it, though.
Note that only adding a new column, and relaxing non-necessary columns to be `NULLABLE` are supported now. Deleting columns, and renaming columns are not supported.

MEMO: [jobs#configuration.load.schemaUpdateOptions](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schemaUpdateOptions) is available
to update the schema of the desitination table as a side effect of the load job, but it is not available for copy job.
Thus, it was not suitable for embulk-output-bigquery idempotence modes, `append`, `replace`, and `replace_backup`, sigh.

## Development

### Run example:

Prepare a json\_keyfile at example/your-project-000.json, then

```
$ embulk bundle install --path vendor/bundle
$ embulk run -X page_size=1 -b . -l trace example/example.yml
```

### Run test:

Place your embulk with `.jar` extension:


```
$ curl -o embulk.jar --create-dirs -L "http://dl.embulk.org/embulk-latest.jar"
$ chmod a+x embulk.jar
```

Investigate JRUBY\_VERSION and Bundler::VERSION included in the embulk.jar:

```
$ echo JRUBY_VERSION | ./embulk.jar irb
2019-08-10 00:59:11.866 +0900: Embulk v0.9.17
Switch to inspect mode.
JRUBY_VERSION
"X.X.X.X"

$ echo "require 'bundler'; Bundler::VERSION" | ./embulk.jar irb
2019-08-10 01:59:10.460 +0900: Embulk v0.9.17
Switch to inspect mode.
require 'bundler'; Bundler::VERSION
"Y.Y.Y"
```

Install the same version of jruby (change X.X.X.X to the version shown above) and bundler:

```
$ rbenv install jruby-X.X.X.X
$ rbenv local jruby-X.X.X.X
$ gem install bundler -v Y.Y.Y
```

Install dependencies (NOTE: Use bundler included in the embulk.jar, otherwise, `gem 'embulk'` is not found):

```
$ ./embulk.jar bundle install --path vendor/bundle
```

Run tests with `env RUBYOPT="-r ./embulk.jar`:

```
$ bundle exec env RUBYOPT="-r ./embulk.jar" rake test
```

To run tests which actually connects to BigQuery such as test/test\_bigquery\_client.rb,
prepare a json\_keyfile at example/your-project-000.json, then

```
$ bundle exec env RUBYOPT="-r ./embulk.jar" ruby test/test_bigquery_client.rb
$ bundle exec env RUBYOPT="-r ./embulk.jar" ruby test/test_example.rb
```

### Release gem:

Change the version of gemspec, and write CHANGELOG.md. Then,

```
$ bundle exec rake release
```

## ChangeLog

[CHANGELOG.md](CHANGELOG.md)
