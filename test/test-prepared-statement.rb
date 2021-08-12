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

class TestPreparedStatement < Test::Unit::TestCase
  def setup
    DuckDB::Database.open do |db|
      db.connect do |connection|
        @connection = connection
        @connection.query("CREATE TABLE users (name text)")
        @connection.query("INSERT INTO users VALUES ('alice'), ('bob')")
        @prepared_statement =
          @connection.prepared_statement("SELECT * FROM users WHERE name = ?")
        yield
      end
    end
  end

  test("#execute_arrow") do
    @prepared_statement.bind(1, "alice")
    assert_equal([Arrow::RecordBatch.new("string" => ["alice"])],
                 @prepared_statement.execute_arrow.to_a)
  end
end
