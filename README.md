
# embulk-output-bigquery

[Embulk](https://github.com/embulk/embulk/) output plugin to load/insert data into [Google BigQuery](https://cloud.google.com/bigquery/)

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
  * Pleast use other product, like [fluent-plugin-bigquery](https://github.com/kaizenplatform/fluent-plugin-bigquery)
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases

Current version of this plugin supports Google API with Service Account Authentication, but does not support
OAuth flow for installed applications.

## Configuration

- **service_account_email**: your Google service account email (string, required)
- **p12_keyfile_path**: fullpath of private key in P12(PKCS12) format (string, required)
- **path_prefix**: (string, required)
- **sequence_format**: (string, optional, default is %03d.%02d)
- **file_ext**: (string, required)
- **source_format**: file type (NEWLINE_DELIMITED_JSON or CSV) (string, required, default is CSV)
- **project**: project_id (string, required)
- **dataset**: dataset (string, required)
- **table**: table name (string, required)
- **auto_create_table**: (boolean, optional default is 0)
- **schema_path**: (string, optional)
- **application_name**: application name anything you like (string, optional)
- **delete_from_local_when_upload_end**: (boolean, optional, default is 0)
- **job_status_max_polling_time**: max job status polling time. (int, optional, default is 3600 sec)
- **job_status_polling_interval**: job status polling interval. (int, optional, default is 10 sec)
- **is_skip_job_result_check**: (boolean, optional, default is 0)
- **field_delimiter**: (string, optional, default is ",")
- **max_bad_records**: (int, optional, default is 0)
- **encoding**: (UTF-8 or ISO-8859-1) (string, optional, default is "UTF-8")

## Example

```yaml
out:
  type: bigquery
  service_account_email: ABCXYZ123ABCXYZ123.gserviceaccount.com
  p12_keyfile_path: /path/to/p12_keyfile.p12
  path_prefix: /path/to/output
  file_ext: csv.gz
  source_format: CSV
  project: your-project-000
  dataset: your_dataset_name
  table: your_table_name
  formatter:
    type: csv
    header_line: false
  encoders:
  - {type: gzip}
```

## Dynamic table creating

When **auto_create_table** is set to true, try to create the table using BigQuery API

To describe the schema of the target table, please write schema path.

```
auto_create_table: true
table: table_name
schema_path: /path/to/schema.json
```

## Build

```
$ ./gradlew gem
```
