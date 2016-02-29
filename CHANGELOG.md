## 0.3.0 - YYYY-MM-DD

Big change is introduced. Now, embulk-output-bigquery is written in JRuby.

* [new feature] Support parallel loads. Fix [#28](https://github.com/embulk/embulk-output-bigquery/issues/28).
* [new feature] Create table first. Fix [#29](https://github.com/embulk/embulk-output-bigquery/issues/29).
* [new feature] Introduce rehearsal mode. Fix [#30](https://github.com/embulk/embulk-output-bigquery/issues/30).
* [new feature] Support `dataset_old` option for `replace_backup`. Fix [#31](https://github.com/embulk/embulk-output-bigquery/issues/31).
* [maintenance] Fix default timestamp format to `%Y-%m-%d %H:%M:%S.%6`. Fix [#32](https://github.com/embulk/embulk-output-bigquery/issues/32).
* [new feature] Support request options such as `timeout_sec`, `open_timeout_sec`, `retries`. Fix [#33](https://github.com/embulk/embulk-output-bigquery/issues/33).
* [new feature] Support continuing from file generation with `skip_file_generation` option.
* [new feature] Guess BigQuery schema from Embulk schema. Fix [#1](https://github.com/embulk/embulk-output-bigquery/issues/1).
* [new feature] Support automatically create dataset.
* [incompatibility change] Formatter plugin support is dropped. Formatter is done in this plugin for specified `source_format`.
* [incompatibility change] Encoder plugin support is dropped. Encoding is done in this plugin for specified `compression`.

## 0.2.3 - 2016-02-19

* [maintenance] Fix detect logic of delete_in_advance mode. [#26](https://github.com/embulk/embulk-output-bigquery/issues/26). @sonots thanks!

## 0.2.2 - 2016-02-15

* [new feature] Added template_table option. [#25](https://github.com/embulk/embulk-output-bigquery/pull/25). @joker1007 thanks!

## 0.2.1 - 2016-01-28

* [maintenance] Upgraded Embulk version to 0.8.1 [#22](https://github.com/embulk/embulk-output-bigquery/pull/22). @joker1007 thanks!
* [maintenance] Formatted code style by checkstyle [#23](https://github.com/embulk/embulk-output-bigquery/pull/23)

## 0.2.0 - 2016-01-26

* [new feature] Added mode parameters and support 4 modes(append, replace, replace_backup, delete_in_advance). [#20](https://github.com/embulk/embulk-output-bigquery/pull/20) [#21](https://github.com/embulk/embulk-output-bigquery/pull/21) @joker1007 thanks!

## 0.1.11 - 2015-11-16

* [maintenance] Change error result display for easy investigation. [#18](https://github.com/embulk/embulk-output-bigquery/pull/18)

## 0.1.10 - 2015-10-06

* [new feature] Added new auth method - json_keyfile of GCP(Google Cloud Platform)'s service account [#17](https://github.com/embulk/embulk-output-bigquery/pull/17)

## 0.1.9 - 2015-08-19

* [maintenance] Upgraded Embulk version to 0.7.1

## 0.1.8 - 2015-08-19

* [new feature] Supported mapreduce-executor. @frsyuki thanks! [#13](https://github.com/embulk/embulk-output-bigquery/pull/13)
* [maintenance] Fixed job_id generation logic [#15](https://github.com/embulk/embulk-output-bigquery/pull/15)
* [maintenance] Refactored [#11](https://github.com/embulk/embulk-output-bigquery/pull/11)

## 0.1.7 - 2015-05-20

* [new feature] Added allow_quoted_newlines option [#10](https://github.com/embulk/embulk-output-bigquery/pull/10)
* [maintenance] Upgraded embulk version to 0.6.8

## 0.1.6 - 2015-04-23

* [new feature] Added ignore_unknown_values option to job_id generation logic. [#9](https://github.com/embulk/embulk-output-bigquery/pull/9)

## 0.1.5 - 2015-04-23

* [new feature] Added ignore_unknown_values option.  [#8](https://github.com/embulk/embulk-output-bigquery/pull/8) @takus thanks!

## 0.1.4 - 2015-04-21

* [new feature] Added prevent_duplicate_insert option

## 0.1.3 - 2015-04-06

* [new feature] Added new auth method - pre-defined access token of GCE(Google Compute Engine)
* [maintenance] Updated Google provided libraries
  * http-client:google-http-client-jackson2 from 1.19.0 to 1.20.0
  * apis:google-api-services-bigquery from v2-rev193-1.19.1 to v2-rev205-1.20.0

## 0.1.2 - 2015-04-01

* [new feature] Changed bulk-load method from "via GCS" to direct-insert
* [new feature] added dynamic table creationg option
