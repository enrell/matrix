#!/usr/bin/env node
"use strict";
/* matrix-doctor: diagnose the Matrix operator environment (no secrets printed). */
const { doctor } = require("../lib/operator");
(async () => {
  let binary = null;
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--binary") binary = args[++i];
  }
  const rep = await doctor(binary);
  console.log(JSON.stringify(rep, null, 2));
  process.exit(rep.cliShapeOk ? 0 : 1);
})();
