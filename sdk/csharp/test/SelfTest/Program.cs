// Self-test runner: component + operator suites, exit = failures.
// `--doctor <binary>`: prints Kernel.Doctor JSON (no secrets) instead.
using System.Text.Json;
using Matrix.Component;

if (args.Length >= 1 && args[0] == "--doctor")
{
    string? binary = args.Length >= 2 ? args[1] : null;
    var rep = Kernel.Doctor(binary);
    Console.WriteLine(JsonSerializer.Serialize(rep,
        new JsonSerializerOptions { WriteIndented = true }));
    var shape = rep.TryGetValue("cli_shape_ok", out var v) &&
        v is bool b && b;
    return shape ? 0 : 1;
}

int fail = 0;
fail += await ComponentSuites.RunAsync();
fail += await OperatorSuites.RunAsync();
Console.WriteLine(fail == 0 ? "selftest: all pass" : $"selftest: {fail} FAILED");
return fail;
