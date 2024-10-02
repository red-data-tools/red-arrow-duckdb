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

class TestConnection < Test::Unit::TestCase
  def setup
    DuckDB::Database.open do |db|
      db.connect do |connection|
        @connection = connection
        yield
      end
    end
  end

  sub_test_case("#query") do
    test("direct") do
      result = @connection.query("SELECT 'data' AS string", output: :arrow)
      assert_equal([Arrow::RecordBatch.new("string" => ["data"])],
                   result.to_a)
    end

    test("prepared statement") do
      @connection.query("CREATE TABLE users (name VARCHAR)")
      @connection.query("INSERT INTO users VALUES ('alice'), ('bob')")
      result = @connection.query("SELECT * FROM users WHERE name = ?",
                                 'alice',
                                 output: :arrow)
      assert_equal([Arrow::RecordBatch.new("name" => ["alice"])],
                   result.to_a)
    end
  end

  test("#query_sql_arrow") do
    result = @connection.query_sql_arrow("SELECT 'data' AS string")
    assert_equal([Arrow::RecordBatch.new("string" => ["data"])],
                 result.to_a)
  end

  test("#register") do
    table = Arrow::Table.new("a" => [1, 2, 3],
                             "b" => [true, false, true])
    @connection.register("data", table) do
      result = @connection.query_sql_arrow("SELECT a FROM data WHERE b")
      assert_equal([
                     Arrow::RecordBatch.new("a" => [1, 3]),
                   ],
                   result.to_a)
    end
  end
end
