
# embulk-output-bigquery

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

| name                      | type        | required?  | default      | description            |  
|:--------------------------|:------------|:-----------|:-------------|:-----------------------|
|  auth_method              | string      | optional   | "private_key"  | `private_key` , `json_key` or `compute_engine`
|  service_account_email    | string      | required when auth_method is private_key  |   | Your Google service account email
|  p12_keyfile              | string      | required when auth_method is private_key   |   | Fullpath of private key in P12(PKCS12) format |
|  json_keyfile             | string      | required when auth_method is json_key     |   | Fullpath of json key |
|  sequence_format          | string      | optional   | %03d.%02d      |  |
|  file_ext                 | string      | optional   |                | e.g. ".csv.gz" ".json.gz" |
|  project                  | string      | required   |                | project_id |
|  dataset                  | string      | required   |                | dataset |
|  table                    | string      | required   |                | table name |
|  auto_create_table        | boolean     | optional   | 0              | [See below](#dynamic-table-creating) |
|  schema_file              | string      | optional   |                | /path/to/schema.json |
|  prevent_duplicate_insert | boolean     | optional   | 0              | [See below](#data-consistency) |
|  delete_from_local_when_job_end | boolean     | optional   | 0            | If set to true, delete local file when job is end |
|  job_status_max_polling_time    | int         | optional   | 3600 sec     | Max job status polling time |
|  job_status_max_polling_time    | int         | optional   | 10 sec       | Job status polling interval |
|  is_skip_job_result_check       | boolean     | optional   | 0            |  |
|  application_name         | string      | optional   | "Embulk BigQuery plugin" | Anything you like |

#### Same options of bq command-line tools or BigQuery job's propery

Following options are same as [bq command-line tools](https://cloud.google.com/bigquery/bq-command-line-tool#creatingtablefromfile) or BigQuery [job's property](https://cloud.google.com/bigquery/docs/reference/v2/jobs#resource).

| name                        | type          | required?    | default        | description                                                                 |
| :-------------------------- | :------------ | :----------- | :------------- | :-----------------------                                                    |
| source_format               | string        | required     | "CSV"          | File type (`NEWLINE_DELIMITED_JSON` or `CSV`)                               |
| max_bad_records             | int           | optional     | 0              |                                                                             |
| field_delimiter             | char          | optional     | ","            |                                                                             |
| encoding                    | string        | optional     | "UTF-8"        | `UTF-8` or `ISO-8859-1`                                                     |
| ignore_unknown_values       | boolean       | optional     | 0              |                                                                             |
| allow_quoted_newlines       | boolean       | optional     | 0              | Set true, if data contains newline characters. It may cause slow procsssing |
| write_disposition           | string        | optional     | "WRITE_APPEND" | `WRITE_APPEND`, `WRITE_TRUNCATE` or `WRITE_EMPTY`                           |

### Example

```yaml
out:
  type: bigquery
  auth_method: private_key   # default
  service_account_email: ABCXYZ123ABCXYZ123.gserviceaccount.com
  p12_keyfile: /path/to/p12_keyfile.p12
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

To describe the schema of the target table, please write schema path.


```yaml
out:
  type: bigquery
  auto_create_table: true
  table: table_%Y_%m
  schema_file: /path/to/schema.json
```

### Data Consistency

When `prevent_duplicate_insert` is set to true, embulk-output-bigquery generate job ID from md5 hash of file  and other options to prevent duplicate data insertion.

`job ID = md5(md5(file) + dataset + table + schema + source_format + file_delimiter + max_bad_records + encoding + ignore_unknown_values + allow_quoted_newlines)`

[job ID must be unique(including failures)](https://cloud.google.com/bigquery/loading-data-into-bigquery#consistency). So same data can't insert with same settings.

In other words, you can retry as many times as you like, in case something bad error(like network error) happens before job insertion.

```yaml
out:
  type: bigquery
  prevent_duplicate_insert: true
```

## Build

```
$ ./gradlew gem
```
