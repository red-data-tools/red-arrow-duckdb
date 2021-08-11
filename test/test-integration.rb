# Copyright 2021  Sutou Kouhei <kou@clear-code.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class TestIntegration < Test::Unit::TestCase
  def setup
    DuckDB::Database.open do |db|
      db.connect do |connection|
        @connection = connection
        yield
      end
    end
  end

  test("query") do
    result = @connection.query_sql_arrow("SELECT 29 AS x")
    loop do
      record_batch = result.fetch
      break if record_batch.nil?
      p record_batch
    end
  end

  test("register") do
    table = Arrow::Table.new("a" => [1, 2, 3],
                             "b" => [true, false, true])
    @connection.register_arrow("data", table) do
       result = @connection.query_sql_arrow("SELECT a FROM data WHERE a > 1")
       loop do
         record_batch = result.fetch
         break if record_batch.nil?
         p record_batch
       end
    end
  end
end
