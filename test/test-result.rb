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

class TestResult < Test::Unit::TestCase
  def setup
    DuckDB::Database.open do |db|
      db.connect do |connection|
        @connection = connection
        sql = "SELECT 29 AS number, 'data' As string"
        @result = @connection.query_sql_arrow(sql)
        yield
      end
    end
  end

  test("#fetch") do
    record_batches = []
    record_batches << @result.fetch
    record_batches << @result.fetch
    assert_equal(
      [
        Arrow::RecordBatch.new("number" => Arrow::Int32Array.new([29]),
                               "string" => ["data"]),
        nil,
      ],
      record_batches)
  end

  test("#each") do
    assert_equal(
      [
        Arrow::RecordBatch.new("number" => Arrow::Int32Array.new([29]),
                               "string" => ["data"]),
      ],
      @result.each.to_a)
  end

  test("#schema") do
    assert_equal(Arrow::Schema.new("number" => Arrow::Int32DataType.new,
                                   "string" => Arrow::StringDataType.new),
                 @result.schema)
  end

  test("#n_columns") do
    assert_equal(2, @result.n_columns)
  end

  test("#n_rows") do
    assert_equal(1, @result.n_rows)
  end

  test("#n_changed_rows") do
    assert_equal(0, @result.n_changed_rows)
  end
end
