defmodule Matrix.Doctor do
  @moduledoc """
  `mix run -e 'Matrix.Doctor.main()'` entrypoint (see `doctor.exs`):
  prints environment diagnosis as JSON (no secrets).
  """

  def main(argv \\ System.argv()) do
    binary = parse(argv)
    rep = Matrix.Operator.doctor(binary)
    IO.puts(:erlang.iolist_to_binary(:json.encode(rep)))
    if rep["cli_shape_ok"], do: System.halt(0), else: System.halt(1)
  end

  defp parse(["--binary", b | _]), do: b
  defp parse([_ | rest]), do: parse(rest)
  defp parse([]), do: nil
end
