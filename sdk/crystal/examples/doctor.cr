# matrix-doctor: diagnose the Matrix operator environment (no secrets printed).
require "../src/matrix-component"

binary = nil
i = 0
while i < ARGV.size
  if ARGV[i] == "--binary"
    binary = ARGV[i + 1]?
    i += 2
  else
    i += 1
  end
end
rep = Matrix::Operator.doctor(binary)
puts rep.to_json
exit(rep["cli_shape_ok"]?.try(&.as_bool?) == true ? 0 : 1)
