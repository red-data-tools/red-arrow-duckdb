#!/usr/bin/env ruby
#
# CC0-1.0: https://creativecommons.org/publicdomain/zero/1.0/deed.en

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
