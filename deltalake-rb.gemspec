require_relative "lib/deltalake/version"

Gem::Specification.new do |spec|
  spec.name          = "deltalake-rb"
  spec.version       = DeltaLake::VERSION
  spec.summary       = "Delta Lake for Ruby"
  spec.homepage      = "https://github.com/ankane/delta-ruby"
  spec.license       = "Apache-2.0"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{ext,lib}/**/*", "Cargo.*"]
  spec.require_path  = "lib"

  spec.required_ruby_version = ">= 3.2"

  # Configure for platform-specific gems
  # For source gem (default platform), include extensions and rb_sys dependency
  if spec.respond_to?(:platform) && spec.platform != Gem::Platform::RUBY
    # This is a precompiled gem - no extension building needed
    spec.files = Dir["*.{md,txt}", "lib/**/*"]
  else
    # This is the source gem - needs extension building
    spec.extensions = ["ext/deltalake/extconf.rb"]
    spec.add_dependency "rb_sys", "~> 0.9"
  end
end
