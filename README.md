
# embulk-output-bigquery

[Embulk](https://github.com/embulk/embulk/) output plugin to load/insert data into [Google BigQuery](https://cloud.google.com/bigquery/) via [GCS(Google Cloud Storage)](https://cloud.google.com/storage/)

## Overview

load data into Google BigQuery as batch jobs via GCS for big amount of data
https://developers.google.com/bigquery/loading-data-into-bigquery

* **Plugin type**: output
* **Resume supported**: no
* **Cleanup supported**: no
* **Dynamic table creating**: todo

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
- **is_file_compressed**: upload file is gzip compressed or not. (boolean, optional, default is 1)
- **bucket**: Google Cloud Storage output bucket name (string, required)
- **remote_path**: folder name in GCS bucket (string, optional)
- **project**: project_id (string, required)
- **dataset**: dataset (string, required)
- **table**: table name (string, required)
- **application_name**: application name anything you like (string, optional)
- **delete_from_local_when_upload_end**: (boolean, optional, default is 0)
- **delete_from_bucket_when_job_end**: (boolean, optional, default is 0)
- **enable_md5hash_check**: enable md5(base64 encoded) hash check between local file and gcs uploaded file. (boolean, optional, default is 1)
- **job_status_max_polling_time**: max job status polling time. (int, optional, default is 3600 sec)
- **job_status_polling_interval**: job status polling interval. (int, optional, default is 10 sec)
- **is_skip_job_result_check**: (boolean, optional, default is 0)

## Support for Google BigQuery Quota policy
embulk-output-bigquery support following [Google BigQuery Quota policy](https://cloud.google.com/bigquery/loading-data-into-bigquery#quota).

* Supported
  * Maximum size per load job: 1TB across all input files
  * Maximum number of files per load job: 10,000
    * embulk-output-bigquery divides a file into more than one job, like below.
      * job1: file1(1GB) file2(1GB)...file10(1GB)
      * job2: file11(1GB) file12(1GB)

* Not Supported
  * Daily limit: 1,000 load jobs per table per day (including failures)
  * 10,000 load jobs per project per day (including failures)

## Example

```yaml
out:
  type: bigquery
  service_account_email: ABCXYZ123ABCXYZ123.gserviceaccount.com
  p12_keyfile_path: /path/to/p12_keyfile.p12
  path_prefix: /path/to/output
  file_ext: csv.gz
  source_format: CSV
  is_file_compressed: 1
  project: your-project-000
  bucket: output_bucket_name
  remote_path: folder_name
  dataset: your_dataset_name
  table: your_table_name
  formatter:
    type: csv
    header_line: false
  encoders:
  - {type: gzip}
```

## Build

```
$ ./gradlew gem
```
