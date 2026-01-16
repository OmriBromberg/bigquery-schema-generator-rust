class BqSchemaGen < Formula
  desc "Generate BigQuery schema from JSON or CSV data"
  homepage "https://github.com/omribromberg/bigquery-schema-generator-rust"
  version "0.1.0"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-aarch64-apple-darwin.tar.gz"
      # sha256 "PLACEHOLDER_SHA256_MACOS_ARM"
    end
    on_intel do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-x86_64-apple-darwin.tar.gz"
      # sha256 "PLACEHOLDER_SHA256_MACOS_INTEL"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-aarch64-unknown-linux-musl.tar.gz"
      # sha256 "PLACEHOLDER_SHA256_LINUX_ARM"
    end
    on_intel do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.0/bq-schema-gen-v0.1.0-x86_64-unknown-linux-musl.tar.gz"
      # sha256 "PLACEHOLDER_SHA256_LINUX_INTEL"
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
