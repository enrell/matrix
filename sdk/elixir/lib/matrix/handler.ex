defmodule Matrix.Handler do
  @moduledoc """
  Component logic behaviour. `on_call/5` runs in its own supervised
  task with a cancellable context; `on_cancel/1` observes cancellation;
  `on_event/2` and `on_stream/3` run on the dispatcher process: observe
  fast, never block it.
  """

  alias Matrix.CallCtx

  @callback on_call(ctx :: CallCtx.t(), ticket :: String.t(), cap :: String.t(), input :: term(), ctx_ref :: reference()) ::
              {:ok, term()} | {:error, String.t(), String.t()}
  @callback on_cancel(ticket :: String.t()) :: any()
  @callback on_event(topic :: String.t(), payload :: term()) :: any()
  @callback on_stream(stream_id :: String.t(), seq :: non_neg_integer(), payload :: String.t()) :: any()

  defmacro __using__(_opts) do
    quote do
      @behaviour Matrix.Handler

      @impl true
      def on_cancel(_ticket), do: :ok

      @impl true
      def on_event(_topic, _payload), do: :ok

      @impl true
      def on_stream(_stream_id, _seq, _payload), do: :ok

      defoverridable on_cancel: 1, on_event: 2, on_stream: 3
    end
  end
end
