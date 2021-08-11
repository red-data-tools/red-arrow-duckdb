# -*- ruby -*-
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

require "bundler/gem_helper"
require "rake/clean"
require "yard"

base_dir = File.join(File.dirname(__FILE__))

helper = Bundler::GemHelper.new(base_dir)
def helper.version_tag
  version
end

helper.install
spec = helper.gemspec

def run_extconf(build_dir, extension_dir, *arguments)
  cd(build_dir) do
    ruby(File.join(extension_dir, "extconf.rb"),
         *arguments)
  end
end

spec.extensions.each do |extension|
  extension_dir = File.join(base_dir, File.dirname(extension))
  build_dir = ENV["BUILD_DIR"]
  if build_dir
    build_dir = File.join(build_dir, "red-arrow-duckdb")
    directory build_dir
  else
    build_dir = extension_dir
  end
  CLOBBER << File.join(build_dir, "Makefile")
  CLOBBER << File.join(build_dir, "mkmf.log")

  makefile = File.join(build_dir, "Makefile")
  file makefile => build_dir do
    run_extconf(build_dir, extension_dir)
  end

  desc "Configure"
  task :configure => build_dir do
    run_extconf(build_dir, extension_dir)
  end

  desc "Compile"
  task :compile => makefile do
    cd(build_dir) do
      sh("make")
    end
  end

  task :clean do
    cd(build_dir) do
      sh("make", "clean") if File.exist?("Makefile")
    end
  end
end

desc "Run tests"
task :test do
  cd(base_dir) do
    ruby("test/run.rb")
  end
end

task default: :test

YARD::Rake::YardocTask.new do |task|
end
