defmodule MatrixComponent.MixProject do
  use Mix.Project

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def project do
    [
      app: :matrix_component,
      version: "0.1.0",
      # OTP 27+ for the :json module (stdlib, no Hex deps); Elixir 1.17+
      # for current task/supervisor semantics. Tested: Elixir 1.20/OTP 29.
      elixir: ">= 1.17.0",
      start_permanent: Mix.env() == :prod,
      # Test support (loopback fake host) must compile in test env.
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: [],
      escript: [main_module: Matrix.Node, name: "mx-node"],
      description: "Matrix external-component SDK for Elixir (ML1 experimental)",
      package: [licenses: ["MIT", "Apache-2.0"], links: %{},
                files: ~w(lib mix.exs README.md LICENSE-MIT LICENSE-APACHE-2.0)]
    ]
  end
end
