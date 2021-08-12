#!/usr/bin/env ruby
#
# CC0-1.0: https://creativecommons.org/publicdomain/zero/1.0/deed.en

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
