class BqSchemaGen < Formula
  desc "Generate BigQuery schema from JSON or CSV data"
  homepage "https://github.com/omribromberg/bigquery-schema-generator-rust"
  version "0.1.0"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-aarch64-apple-darwin.tar.gz"
      sha256 "775465dd1819a82f1605eaa6a3274a460c64f2930483fc37a449b76e38ad2282"
    end
    on_intel do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-x86_64-apple-darwin.tar.gz"
      sha256 "db8bde072ba06f032534409da2dcd90860a409407441d9f070768ca50ea35f34"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-aarch64-unknown-linux-musl.tar.gz"
      sha256 "10e1f345e0ab4b1d1f0f1f640a6753d7143d271db1c223420f19d8879c8e1ce3"
    end
    on_intel do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-x86_64-unknown-linux-musl.tar.gz"
      sha256 "19112911508d3b1d8e9e671c81f2a907af0396472df96f7947d0cc512b57bbe3"
    end
  end

  def install
    bin.install "bq-schema-gen"

    # Install shell completions if present
    if File.exist?("completions/bq-schema-gen.bash")
      bash_completion.install "completions/bq-schema-gen.bash"
    end
    if File.exist?("completions/_bq-schema-gen")
      zsh_completion.install "completions/_bq-schema-gen"
    end
    if File.exist?("completions/bq-schema-gen.fish")
      fish_completion.install "completions/bq-schema-gen.fish"
    end

    # Install man page if present
    if File.exist?("man/bq-schema-gen.1")
      man1.install "man/bq-schema-gen.1"
    end
  end

  test do
    # Test basic functionality
    (testpath/"test.json").write('{"name": "test", "value": 42}')
    output = shell_output("#{bin}/bq-schema-gen #{testpath}/test.json 2>/dev/null")
    assert_match "name", output
    assert_match "STRING", output
  end
end
