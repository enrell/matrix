defmodule Matrix.Framing do
  @moduledoc """
  Length-prefixed JSON framing over a Unix socket (matrix.component/0.1).

  Frames are only consumed when a reader waits: coalesced arrivals stay
  buffered for the next read. Malformed frames drop silently; the
  session survives.
  """

  @max_frame 1_048_576

  def max_frame, do: @max_frame

  @doc "Connects to a Unix socket path."
  def connect(path) do
    :gen_tcp.connect({:local, String.to_charlist(path)}, 0,
      [:binary, packet: :raw, active: false]
    )
  end

  @doc "Sends one envelope map."
  def send_frame(sock, map, max_frame \\ @max_frame) do
    # :json.encode returns iodata: flatten before framing.
    raw = :erlang.iolist_to_binary(:json.encode(stringify(map)))

    if byte_size(raw) > max_frame do
      {:error, :frame_above_max}
    else
      :gen_tcp.send(sock, [<<byte_size(raw)::32-big>>, raw])
    end
  end

  @doc """
  Reads one envelope. Returns `{:ok, map}`, `{:malformed}` (drop, keep
  going), `{:eof}` or `{:error, reason}`.
  """
  def read_frame(sock, max_frame \\ @max_frame, timeout \\ 30_000) do
    with {:ok, <<n::32-big>>} <- recv_exact(sock, 4, timeout),
         true <- n > 0 and n <= max_frame,
         {:ok, payload} <- recv_exact(sock, n, timeout) do
      {:ok, atomize(:json.decode(payload))}
    else
      :eof -> {:eof}
      false -> {:error, :bad_frame_length}
      {:error, reason} -> {:error, reason}
      _ -> {:malformed}
    end
  rescue
    _ -> {:malformed}
  end

  defp recv_exact(_sock, 0, _timeout), do: {:ok, <<>>}

  defp recv_exact(sock, n, timeout) do
    recv_exact(sock, n, timeout, <<>>)
  end

  defp recv_exact(_sock, 0, _timeout, acc), do: {:ok, acc}

  defp recv_exact(sock, n, timeout, acc) do
    case :gen_tcp.recv(sock, n, timeout) do
      {:ok, data} ->
        rest = n - byte_size(data)
        if rest == 0, do: {:ok, acc <> data}, else: recv_exact(sock, rest, timeout, acc <> data)

      {:error, :closed} ->
        if acc == <<>>, do: :eof, else: {:error, :truncated}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # :json returns binary-keyed maps already; normalize lists/values.
  defp atomize(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {k, atomize(v)} end)
  end

  defp atomize(list) when is_list(list), do: Enum.map(list, &atomize/1)
  defp atomize(other), do: other

  defp stringify(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {to_string(k), stringify(v)} end)
  end

  defp stringify(list) when is_list(list), do: Enum.map(list, &stringify/1)
  defp stringify(other), do: other

  @doc "True when both parse as u64 and are equal (no precision loss)."
  def gen_equal?(a, b) do
    with {x, ""} <- Integer.parse(to_string(a)),
         {y, ""} <- Integer.parse(to_string(b)),
         true <- x >= 0 and y >= 0 and x < 18_446_744_073_709_551_616 and y < 18_446_744_073_709_551_616 do
      x == y
    else
      _ -> false
    end
  end
end
