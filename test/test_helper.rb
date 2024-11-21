require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "polars-df"

class Minitest::Test
  def with_new_table
    prefix = ENV["CLOUD_PREFIX"]

    if prefix
      if prefix.start_with?("s3://")
        ENV["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
      end
      yield "#{prefix}/delta-ruby-test/#{Time.now.to_f}"
    else
      Dir.mktmpdir do |table_uri|
        yield table_uri
      end
    end
  end
end
