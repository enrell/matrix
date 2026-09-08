defmodule Matrix.Errors do
  @moduledoc """
  Structured errors: stable code, phase, safe message (ML1, no secrets).
  """

  defmodule SdkError do
    @moduledoc "Base SDK error."
    defexception [:code, :phase, :message]

    @impl true
    def message(%{code: c, phase: p, message: m}), do: "#{c} [#{p}]: #{m}"
  end

  defmodule DepError do
    @moduledoc "Dependency-call error: the wire code passes through untouched."
    defexception [:code, :message]

    @impl true
    def message(%{code: c, message: m}), do: "#{c} [dependency]: #{m}"
  end

  defmodule ResError do
    @moduledoc "Activation resource error."
    defexception [:code, :message]

    @impl true
    def message(%{code: c, message: m}), do: "#{c} [resource]: #{m}"
  end

  defmodule BootstrapError do
    @moduledoc "start/connect failure. Nothing owned is left behind."
    defexception [:code, :phase, :message]

    @impl true
    def message(%{code: c, phase: p, message: m}), do: "#{c} [#{p}]: #{m}"
  end

  defmodule OperatorError do
    @moduledoc """
    Admin refusal/failure. Wire codes pass through; timeouts report
    outcome-unknown and never retry implicitly.
    """
    defexception [:code, :message]

    @impl true
    def message(%{code: c, message: m}), do: "#{c} [request]: #{m}"
  end

  defmodule BusinessError do
    @moduledoc "Raised by handlers: answered with this code/message."
    defexception [:code, :message]

    @impl true
    def message(%{code: c, message: m}), do: "#{c}: #{m}"
  end
end
