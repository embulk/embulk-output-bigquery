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

### INCOMPATIBILITY CHANGES

v0.3.x has incompatibility changes with v0.2.x. Please see [CHANGELOG.md](CHANGELOG.md) for details.

* `formatter` option (formatter plugin support) is dropped. Use `source_format` option instead. (it already exists in v0.2.x too)
* `encoders` option (encoder plugin support) is dropped. Use `compression` option instead (it already exists in v0.2.x too).
* `mode: append` mode now expresses a transactional append, and `mode: append_direct` is one which is not transactional.

## Configuration

#### Original options

| name                                 | type        | required?  | default                  | description            |  
|:-------------------------------------|:------------|:-----------|:-------------------------|:-----------------------|
|  mode                                | string      | optional   | "append"                 | See [Mode](#mode)     |
|  auth_method                         | string      | optional   | "private_key"            | `private_key` , `json_key` or `compute_engine`
|  service_account_email               | string      | required when auth_method is private_key  |   | Your Google service account email
|  p12_keyfile                         | string      | required when auth_method is private_key  |   | Fullpath of private key in P12(PKCS12) format |
|  json_keyfile                        | string      | required when auth_method is json_key     |   | Fullpath of json key |
|  project                             | string      | required if json_keyfile is not given     |   | project_id |
|  dataset                             | string      | required   |                          | dataset |
|  table                               | string      | required   |                          | table name, or table name with a partition decorator such as `table_name$20160929`|
|  auto_create_dataset                 | boolean     | optional   | false                    | automatically create dataset |
|  auto_create_table                   | boolean     | optional   | false                    | See [Dynamic Table Creating](#dynamic-table-creating) |
|  schema_file                         | string      | optional   |                          | /path/to/schema.json |
|  template_table                      | string      | optional   |                          | template table name. See [Dynamic Table Creating](#dynamic-table-creating) |
|  prevent_duplicate_insert            | boolean     | optional   | false                    | See [Prevent Duplication] (#prevent-duplication) |
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
|  time_partitioning.expiration_ms | int      | optional  | nil     | Number of milliseconds for which to keep the storage for a partition. partition |
|  schema_update_options            | array    | optional  | nil     | (Experimental) List of `ALLOW_FIELD_ADDITION` or `ALLOW_FIELD_RELAXATION` or both. See [jobs#configuration.load.schemaUpdateOptions](https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schemaUpdateOptions). NOTE for the current status: `schema_update_options` does not work for `copy` job, that is, is not effective for most of modes such as `append`, `append_direct`, `replace`, `replace_backup` (except `delete_in_advance`) |

### Example

```yaml
out:
  type: bigquery
  mode: append
  auth_method: private_key   # default
  service_account_email: ABCXYZ123ABCXYZ123.gserviceaccount.com
  p12_keyfile: /path/to/p12_keyfile.p12
  project: your-project-000
  dataset: your_dataset_name
  table: your_table_name
  compression: GZIP
  source_format: NEWLINE_DELIMITED_JSON
```

### mode

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

##### replace_backup

1. Load to temporary table (Create and WRITE_APPEND in parallel)
2. Copy destination table (or partition) to backup table (or partition). (dataset_old, table_old)
3. Copy temporary table to destination table (or partition). (WRITE_TRUNCATE)

```is_skip_job_result_check``` must be false when replace_backup mode.

##### delete_in_advance

1. Delete destination table (or partition), if it exists.
2. Load to destination table (or partition).

### Authentication

There are three methods supported to fetch access token for the service account.

1. Public-Private key pair of GCP(Google Cloud Platform)'s service account
2. JSON key of GCP(Google Cloud Platform)'s service account
3. Pre-defined access token (Google Compute Engine only)

#### Public-Private key pair of GCP's service account

You first need to create a service account (client ID),
download its private key and deploy the key with embulk.

```yaml
out:
  type: bigquery
  auth_method: private_key   # default
  service_account_email: ABCXYZ123ABCXYZ123.gserviceaccount.com
  p12_keyfile: /path/to/p12_keyfile.p12
```

#### JSON key of GCP's service account

You first need to create a service account (client ID),
download its json key and deploy the key with embulk.

```yaml
out:
  type: bigquery
  auth_method: json_key
  json_keyfile: /path/to/json_keyfile.json
```

You can also embed contents of json_keyfile at config.yml.

```yaml
out:
  type: bigquery
  auth_method: json_key
  json_keyfile:
    content: |
      {
          "private_key_id": "123456789",
          "private_key": "-----BEGIN PRIVATE KEY-----\nABCDEF",
          "client_email": "..."
       }
```

#### Pre-defined access token(GCE only)

On the other hand, you don't need to explicitly create a service account for embulk when you
run embulk in Google Compute Engine. In this third authentication method, you need to
add the API scope "https://www.googleapis.com/auth/bigquery" to the scope list of your
Compute Engine VM instance, then you can configure embulk like this.

```yaml
out:
  type: bigquery
  auth_method: compute_engine
```

### Table id formatting

`table` and option accept [Time#strftime](http://ruby-doc.org/core-1.9.3/Time.html#method-i-strftime)
format to construct table ids.
Table ids are formatted at runtime
using the local time of the embulk server.

For example, with the configuration below,
data is inserted into tables `table_2015_04`, `table_2015_05` and so on.

```yaml
out:
  type: bigquery
  table: table_%Y_%m
```

### Dynamic table creating

When `auto_create_table` is set to true, try to create the table using BigQuery API.

If table already exists, insert into it.

There are 3 ways to set schema.

#### Set schema.json

Please set file path of schema.json.

```yaml
out:
  type: bigquery
  auto_create_table: true
  table: table_%Y_%m
  schema_file: /path/to/schema.json
```

#### Set template_table in dataset

Plugin will try to read schema from existing table and use it as schema template.

```yaml
out:
  type: bigquery
  auto_create_table: true
  table: table_%Y_%m
  template_table: existing_table_name
```

#### Guess from Embulk Schema

Plugin will try to guess BigQuery schema from Embulk schema.  It is also configurable with `column_options`. See [Column Options](#column-options).

### Column Options

Column options are used to aid guessing BigQuery schema, or to define conversion of values:

- **column_options**: advanced: an array of options for columns
  - **name**: column name
  - **type**: BigQuery type such as `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP`, and `RECORD`. See belows for supported conversion type.
    - boolean:   `BOOLEAN`, `STRING` (default: `BOOLEAN`)
    - long:      `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP` (default: `INTEGER`)
    - double:    `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP` (default: `FLOAT`)
    - string:    `BOOLEAN`, `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP`, `RECORD` (default: `STRING`)
    - timestamp: `INTEGER`, `FLOAT`, `STRING`, `TIMESTAMP` (default: `TIMESTAMP`)
    - json:      `STRING`,  `RECORD` (default: `STRING`)
  - **mode**: BigQuery mode such as `NULLABLE`, `REQUIRED`, and `REPEATED` (string, default: `NULLABLE`)
  - **fields**: Describes the nested schema fields if the type property is set to RECORD. Please note that this is **required** for `RECORD` column.
  - **timestamp_format**: timestamp format to convert into/from `timestamp` (string, default is `default_timestamp_format`)
  - **timezone**: timezone to convert into/from `timestamp` (string, default is `default_timezone`).
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

```
filters:
  - type: to_json
    column: {name: payload, type: string}
    default_format: "%Y-%m-%d %H:%M:%S.%6N"
out:
  type: bigquery
  payload_column_index: 0 # or, payload_column: payload
```

Furtheremore, if your files are originally jsonl or csv files, you can even skip a parser with [embulk-parser-none](https://github.com/sonots/embulk-parser-none) as:

```
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

### Prevent Duplication

`prevent_duplicate_insert` option is used to prevent inserting same data for modes `append` or `append_direct`.

When `prevent_duplicate_insert` is set to true, embulk-output-bigquery generate job ID from md5 hash of file and other options.

`job ID = md5(md5(file) + dataset + table + schema + source_format + file_delimiter + max_bad_records + encoding + ignore_unknown_values + allow_quoted_newlines)`

[job ID must be unique(including failures)](https://cloud.google.com/bigquery/loading-data-into-bigquery#consistency) so that same data can't be inserted with same settings repeatedly.

```yaml
out:
  type: bigquery
  prevent_duplicate_insert: true
```

### GCS Bucket

This is useful to reduce number of consumed jobs, which is limited by [10,000 jobs per project per day](https://cloud.google.com/bigquery/quota-policy#import).

This plugin originally loads local files into BigQuery in parallel, that is, consumes a number of jobs, say 24 jobs on 24 CPU core machine for example (this depends on embulk parameters such as `min_output_tasks` and `max_threads`).

BigQuery supports loading multiple files from GCS with one job (but not from local files, sigh), therefore, uploading local files to GCS and then loading from GCS into BigQuery reduces number of consumed jobs.

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
  auto_create_table: true
```

You may configure `time_partitioning` parameter together to create table via `auto_create_table: true` option as:

```yaml
out:
  type: bigquery
  table: table_name$20160929
  auto_create_table: true
  time_partitioning:
    type: DAY
    expiration_ms: 259200000
```

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

```
$ bundle exec rake test
```

To run tests which actually connects to BigQuery such as test/test\_bigquery\_client.rb,
prepare a json\_keyfile at example/your-project-000.json, then

```
$ bundle exec ruby test/test_bigquery_client.rb
$ bundle exec ruby test/test_example.rb
```

### Release gem:

Fix gemspec, then

```
$ bundle exec rake release
```

## ChangeLog

[CHANGELOG.md](CHANGELOG.md)
