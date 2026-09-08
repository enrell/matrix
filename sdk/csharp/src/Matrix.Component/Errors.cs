// Structured errors: stable code, phase, safe message (ML1, no secrets).
namespace Matrix.Component;

/// <summary>Base SDK error.</summary>
public class SdkError : Exception
{
    public string Code { get; }
    public string Phase { get; }
    public string Detail { get; }

    public SdkError(string code, string phase, string message) : base($"{code} [{phase}]: {message}")
    {
        Code = code;
        Phase = phase;
        Detail = message;
    }
}

/// <summary>Dependency-call error: the wire code passes through untouched.</summary>
public sealed class DepError : SdkError
{
    public DepError(string code, string message) : base(code, "dependency", message) { }
}

/// <summary>Activation resource error.</summary>
public sealed class ResError : SdkError
{
    public ResError(string code, string message) : base(code, "resource", message) { }
}

/// <summary>Start/connect failure. Nothing owned is left behind.</summary>
public sealed class BootstrapError : SdkError
{
    public BootstrapError(string code, string phase, string message) : base(code, phase, message) { }
}

/// <summary>
/// Admin refusal/failure. Wire codes pass through; timeouts report
/// outcome-unknown and never retry implicitly.
/// </summary>
public sealed class OperatorError : SdkError
{
    public OperatorError(string code, string message) : base(code, "request", message) { }
}

/// <summary>Business error thrown by handlers: answered with this code/message.</summary>
public sealed class ComponentError : Exception
{
    public string Code { get; }

    public ComponentError(string code, string message) : base(message)
    {
        Code = code;
    }
}
