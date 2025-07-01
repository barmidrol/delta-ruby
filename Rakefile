require "bundler/gem_tasks"
require "rake/testtask"
require "rake/extensiontask"

task default: :test
Rake::TestTask.new do |t|
  t.libs << "test"
  t.pattern = "test/**/*_test.rb"
end

platforms = [
  "x86_64-linux",
  "x86_64-linux-musl",
  "aarch64-linux",
  "aarch64-linux-musl",
  "x86_64-darwin",
  "arm64-darwin",
  "x64-mingw-ucrt"
]

gemspec = Bundler.load_gemspec("deltalake-rb.gemspec")
Rake::ExtensionTask.new("deltalake", gemspec) do |ext|
  ext.lib_dir = "lib/deltalake"
  ext.cross_compile = true
  ext.cross_platform = platforms
  ext.cross_compiling do |spec|
    spec.dependencies.reject! { |dep| dep.name == "rb_sys" }
    spec.files.reject! { |file| File.fnmatch?("ext/*", file, File::FNM_EXTGLOB) }
  end
end

task :remove_ext do
  path = "lib/deltalake/deltalake.bundle"
  File.unlink(path) if File.exist?(path)
end

Rake::Task["build"].enhance [:remove_ext]

# Add tasks for precompiled gem development
namespace :precompiled do
  desc "Build precompiled gems for all platforms (requires Docker)"
  task :build do
    platforms.each do |platform|
      puts "Building for #{platform}..."
      sh "bundle exec rb-sys-dock --platform #{platform} --build"
    end
  end

  desc "Build precompiled gem for current platform"
  task :build_current do
    current_platform = case RUBY_PLATFORM
    when /x86_64-linux/
      "x86_64-linux"
    when /aarch64-linux|arm64-linux/
      "aarch64-linux"
    when /x86_64-darwin/
      "x86_64-darwin"
    when /arm64-darwin|aarch64-darwin/
      "arm64-darwin"
    when /x64-mingw/
      "x64-mingw-ucrt"
    else
      puts "Unsupported platform: #{RUBY_PLATFORM}"
      next
    end

    puts "Building for #{current_platform}..."
    sh "bundle exec rb-sys-dock --platform #{current_platform} --build"
  end

  desc "Test installation of precompiled gem"
  task :test_install do
    # Build a gem for the current platform
    Rake::Task["precompiled:build_current"].invoke

    # Find the built gem
    gem_file = Dir["pkg/*-#{current_platform}.gem"].first
    if gem_file
      puts "Testing installation of #{gem_file}..."
      sh "gem install #{gem_file}"
      puts "Successfully installed precompiled gem!"
    else
      puts "No precompiled gem found for current platform"
    end
  end
end
