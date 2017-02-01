# ChangeLog

## v2.0.0

* Use Fluentd v0.14 API and drop Fluentd v0.12 or earlier support

## v0.6.2

### Fixes

* Handle timeout event properly when buffer is empty
* Match both `multiline_start_regexp` and `multiline_end_regexp` properly

## v0.6.0

### Improvements

* Wait next line forever when `flush_interval` is 0
* Add `use_first_timestamp`

### Incompatibilities

* Flush buffer when match both `multiline_start_regexp` and `multiline_end_regexp`

