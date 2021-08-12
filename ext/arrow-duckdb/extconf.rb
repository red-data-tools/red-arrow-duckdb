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

require "extpp"
require "mkmf-gnome"
require "native-package-installer"

required_pkg_config_package("arrow-glib")
required_pkg_config_package("arrow-dataset",
                            debian: "libarrow-dataset-dev",
                            redhat: "arrow-dataset-devel")

[
  ["glib2", "ext/glib2"],
  ["duckdb", "ext/duckdb"],
].each do |name, source_dir|
  spec = find_gem_spec(name)
  source_dir = File.join(spec.full_gem_path, source_dir)
  build_dir = source_dir
  add_depend_package_path(name, source_dir, build_dir)
end

have_library("duckdb") or exit(false)

create_makefile("arrow_duckdb")
