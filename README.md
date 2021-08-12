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

### Receive result as Apache Arrow data

```ruby
require "arrow-duckdb"

DuckDB::Database.open do |db|
  db.connect do |connection|
    connection.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')

    connection.query("INSERT into users VALUES(1, 'Alice')")
    connection.query("INSERT into users VALUES(2, 'Bob')")
    connection.query("INSERT into users VALUES(3, 'Cathy')")

    result = connection.query("SELECT * FROM users", output: :arrow)
    puts(result.to_table)
    # 	id	name
    # 0	 1	Alice
    # 1	 2	Bob 
    # 2	 3	Cathy
  end
end
```

### Use Apache Arrow data as input

```ruby
require "arrow-duckdb"

users = Arrow::Table.new("id" => [1, 2, 3],
                         "name" => ["Alice", "Bob", "Cathy"])
DuckDB::Database.open do |db|
  db.connect do |connection|
    connection.register("users", users) do
      connection.query("SELECT * FROM users").each do |row|
        p row
        # ["1", "Alice"]
        # ["2", "Bob"]
        # ["3", "Cathy"]
      end
    end
  end
end
```

### Filter Apache Arrow data by DuckDB

```ruby
require "arrow-duckdb"

users = Arrow::Table.new("id" => [1, 2, 3],
                         "name" => ["Alice", "Bob", "Cathy"])
DuckDB::Database.open do |db|
  db.connect do |connection|
    filtered_users = connection.register("users", users) do
      result = connection.query("SELECT * FROM users WHERE id > ?",
                                1,
                                output: :arrow)
      result.to_table
    end
    puts(filtered_users)
    # 	id	name
    # 0	 2	Bob 
    # 1	 3	Cathy

    # Use filtered data again
    connection.register("filtered_users", filtered_users) do
      result = connection.query("SELECT * FROM filtered_users",
                                output: :arrow)
      puts(result.to_table)
      # 	id	name
      # 0	 2	Bob 
      # 1	 3	Cathy
    end
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
