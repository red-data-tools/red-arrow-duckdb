# README

## Name

Red Arrow DuckDB

## Description

Red Arrow DuckDB is a library that provides Apache Arrow support to ruby-duckdb.

## Install

```bash
gem install red-arrow-duckdb
```

## Usage

```ruby
require "arrow-duckdb"

DuckDB::Database.open do |db|
  db.connect do |connection|
    p connection
  end
end
```

## Dependencies

* [Red Arrow](https://github.com/apache/arrow/tree/master/ruby/red-arrow)

* [ruby-duckdb](https://github.com/suketa/ruby-duckdb)

## Authors

* Sutou Kouhei \<kou@clear-code.com\>

## License

Apache License 2.0. See doc/text/apache-2.0.txt for details.

(Sutou Kouhei has a right to change the license including contributed
patches.)
