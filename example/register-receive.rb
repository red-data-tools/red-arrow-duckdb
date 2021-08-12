#!/usr/bin/env ruby
#
# CC0-1.0: https://creativecommons.org/publicdomain/zero/1.0/deed.en

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
