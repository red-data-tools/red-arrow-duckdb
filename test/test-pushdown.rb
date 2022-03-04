# Copyright 2022  Sutou Kouhei <kou@clear-code.com>
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

class TestPushdown < Test::Unit::TestCase
  def setup
    DuckDB::Database.open do |db|
      db.connect do |connection|
        @connection = connection
        yield
      end
    end
  end

  test("timestamp") do
    table = Arrow::Table.new("value" => [
                               Time.utc(2022, 3, 4, 0, 0, 0),
                               Time.utc(2022, 3, 5, 0, 0, 0),
                               Time.utc(2022, 3, 6, 0, 0, 0),
                             ])
    @connection.register("data", table) do
      result = @connection.query_sql_arrow(<<-SQL)
SELECT value FROM data WHERE value >= '2022-03-05'
      SQL
      assert_equal(Arrow::Table.new("value" => [
                                      Time.utc(2022, 3, 5, 0, 0, 0),
                                      Time.utc(2022, 3, 6, 0, 0, 0),
                                    ]),
                   result.to_table)
    end
  end
end
