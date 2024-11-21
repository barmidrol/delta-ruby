# delta-ruby

[Delta Lake](https://delta.io/) for Ruby

Supports local files and Amazon S3

[![Build Status](https://github.com/ankane/delta-ruby/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/delta-ruby/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "deltalake-rb"
```

It can take 5-10 minutes to compile the gem.

## Getting Started

Write data

```ruby
df = Polars::DataFrame.new({"a" => [1, 2], "b" => [3.0, 4.0]})
DeltaLake.write("./data/delta", df)
```

Load a table

```ruby
dt = DeltaLake::Table.new("./data/delta")
df2 = dt.to_polars
```

Get a lazy frame

```ruby
lf = dt.to_polars(eager: false)
```

Append rows

```ruby
DeltaLake.write("./data/delta", df, mode: "append")
```

Overwrite a table

```ruby
DeltaLake.write("./data/delta", df, mode: "overwrite")
```

Delete rows

```ruby
dt.delete("a > 1")
```

Vacuum

```ruby
dt.vacuum(dry_run: false)
```

Load a previous version of a table

```ruby
dt = DeltaLake::Table.new("./data/delta", version: 1)
# or
dt.load_as_version(1)
```

Get the schema

```ruby
dt.schema
```

Get metadata

```ruby
dt.metadata
```

## API

This library follows the [Delta Lake Python API](https://delta-io.github.io/delta-rs/) (with a few changes to make it more Ruby-like). You can follow Python tutorials and convert the code to Ruby in many cases. Feel free to open an issue if you run into problems.

## History

View the [changelog](https://github.com/ankane/delta-ruby/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/delta-ruby/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/delta-ruby/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/delta-ruby.git
cd delta-ruby
bundle install
bundle exec rake compile
bundle exec rake test
```
