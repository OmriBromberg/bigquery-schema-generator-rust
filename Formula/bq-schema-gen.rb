class BqSchemaGen < Formula
  desc "Generate BigQuery schema from JSON or CSV data"
  homepage "https://github.com/omribromberg/bigquery-schema-generator-rust"
  version "0.1.1"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.1/bq-schema-gen-v0.1.1-aarch64-apple-darwin.tar.gz"
      sha256 "c8c2bc3e75f652aad684ebea8e0598a05afc7a54737d700425c00cb6bf396f37"
    end
    on_intel do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.1/bq-schema-gen-v0.1.1-x86_64-apple-darwin.tar.gz"
      sha256 "c24f5804e3b71474e2f5348ac930039c749b0cdc535dbdd3c2dfebdbe3857c1a"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.1/bq-schema-gen-v0.1.1-aarch64-unknown-linux-musl.tar.gz"
      sha256 "4685350a1a6a8c5afc129af414b110ab8f05b3df2aee10c770c20d4fa92be52f"
    end
    on_intel do
      url "https://github.com/omribromberg/bigquery-schema-generator-rust/releases/download/v0.1.1/bq-schema-gen-v0.1.1-x86_64-unknown-linux-musl.tar.gz"
      sha256 "b529f46cbbff457e7468f8a9fb6fed45903f19161c95128ea86067d37429e39a"
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
