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

module ArrowDuckDB
  module ArrowableQuery
    def query(sql, *args, output: nil)
      return super(sql, *args) if output != :arrow

      return query_sql_arrow(sql) if args.empty?

      stmt = PreparedStatement.new(self, sql)
      args.each_with_index do |arg, i|
        stmt.bind(i + 1, arg)
      end
      stmt.execute_arrow
    end
  end
end

DuckDB::Connection.prepend(ArrowDuckDB::ArrowableQuery)
