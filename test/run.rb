#!/usr/bin/env ruby
#
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

$VERBOSE = true

require "pathname"

base_dir = Pathname.new(__FILE__).dirname.parent.expand_path

ext_dir = base_dir + "ext" + "arrow-duckdb"
lib_dir = base_dir + "lib"
test_dir = base_dir + "test"

$LOAD_PATH.unshift(ext_dir.to_s)
$LOAD_PATH.unshift(lib_dir.to_s)

if system("type make > /dev/null")
  Dir.chdir(ext_dir) do
    if File.exist?("Makefile")
      system("make -j8 > /dev/null") or exit(false)
    end
  end
end

require_relative "helper"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
