const BASE_URL = "https://integracoes.bitlab.net.br/webhook/notificacao_biovita";
const DEFAULT_TIPO = "A";

function getApiKey() {
  return process.env.BITLAB_API_KEY || process.env.BIOVITA_API_KEY || null;
}

function parseArgs() {
  const args = process.argv.slice(2);
  const mode = args[0] || "auth";
  const tipoIndex = args.indexOf("--tipo");
  const tipo = tipoIndex >= 0 ? args[tipoIndex + 1] : DEFAULT_TIPO;

  return {
    mode,
    tipo,
    allowPending: args.includes("--allow-pending"),
  };
}

function normalizePayload(json) {
  return Array.isArray(json) ? json[0] : json;
}

function redactItem(item) {
  if (!item || typeof item !== "object") return item;

  return {
    ...item,
    NM_PACIENTE: item.NM_PACIENTE ? "[redacted]" : item.NM_PACIENTE,
    NU_CELULAR: item.NU_CELULAR ? "[redacted]" : item.NU_CELULAR,
  };
}

function summarizeData(data) {
  const tipoCounts = {};
  const dates = new Set();
  const ids = [];

  for (const item of data) {
    tipoCounts[item.ID_TIPO] = (tipoCounts[item.ID_TIPO] || 0) + 1;

    if (item.DT_INC_INF) {
      dates.add(String(item.DT_INC_INF).substring(0, 10));
    }

    ids.push({
      CD_SMS: item.CD_SMS ?? null,
      CD_PACIENTE: item.CD_PACIENTE ?? null,
      CD_REQUISICAO: item.CD_REQUISICAO ?? null,
      ID_TIPO: item.ID_TIPO ?? null,
      BO_ENVIADO: item.BO_ENVIADO ?? null,
    });
  }

  return {
    tipoCounts,
    dates: [...dates].sort(),
    ids,
    firstItemRedacted: redactItem(data[0] || null),
  };
}

async function requestNotifications({ apiKey, tipo, status }) {
  const url = `${BASE_URL}?tipo=${encodeURIComponent(tipo)}&status=${encodeURIComponent(status)}`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "X-API-KEY": apiKey,
      Accept: "application/json",
    },
    signal: AbortSignal.timeout(30000),
  });

  const text = await response.text();
  let json = null;

  try {
    json = JSON.parse(text);
  } catch {}

  const payload = normalizePayload(json);

  return {
    url,
    httpStatus: response.status,
    text,
    payload,
  };
}

function printResult(label, result) {
  console.log(`\n=== ${label} ===`);
  console.log(`URL: ${result.url}`);
  console.log(`HTTP Status: ${result.httpStatus}`);

  if (!result.payload || typeof result.payload !== "object") {
    console.log(`Resposta nao JSON: ${result.text.slice(0, 500)}`);
    return;
  }

  console.log(`success: ${result.payload.success}`);
  console.log(`count: ${result.payload.count}`);
  console.log(`message: ${result.payload.message ?? null}`);

  if (Array.isArray(result.payload.data)) {
    console.log(JSON.stringify(summarizeData(result.payload.data), null, 2));
  }
}

async function runAuthValidation(apiKey) {
  const tipos = ["*", "A", "P", "R", "N"];

  for (const tipo of tipos) {
    const result = await requestNotifications({ apiKey, tipo, status: "enviado" });
    printResult(`Consulta segura: tipo=${tipo}, status=enviado`, result);
  }
}

async function runPendingValidation(apiKey, tipo) {
  console.log("\nATENCAO: este modo usa status=pendente.");
  console.log("A primeira chamada pode marcar os registros retornados como enviados.");
  console.log("A segunda chamada valida se eles deixaram de aparecer como pendentes.\n");

  const first = await requestNotifications({ apiKey, tipo, status: "pendente" });
  printResult(`1a chamada pendente: tipo=${tipo}`, first);

  const second = await requestNotifications({ apiKey, tipo, status: "pendente" });
  printResult(`2a chamada pendente: tipo=${tipo}`, second);

  const firstCount = Number(first.payload?.count ?? 0);
  const secondCount = Number(second.payload?.count ?? 0);

  console.log("\n=== Validacao pendente ===");
  if (first.httpStatus !== 200 || second.httpStatus !== 200) {
    console.log("Nao foi possivel validar porque uma das chamadas nao retornou HTTP 200.");
  } else if (firstCount > 0 && secondCount === 0) {
    console.log("OK: havia pendentes na primeira chamada e eles nao apareceram mais na segunda.");
  } else if (firstCount === 0) {
    console.log("Nao havia pendentes na primeira chamada; comportamento de marcacao nao foi exercitado.");
  } else {
    console.log("ATENCAO: ainda existem pendentes apos a segunda chamada.");
  }
}

async function main() {
  const { mode, tipo, allowPending } = parseArgs();
  const apiKey = getApiKey();

  if (!apiKey) {
    throw new Error("Informe BITLAB_API_KEY no ambiente.");
  }

  if (mode === "auth") {
    await runAuthValidation(apiKey);
    return;
  }

  if (mode === "pending") {
    if (!allowPending) {
      console.log("Modo pending bloqueado por seguranca.");
      console.log(`Para executar: node prod_notifications_validation.js pending --tipo ${tipo} --allow-pending`);
      process.exitCode = 1;
      return;
    }

    await runPendingValidation(apiKey, tipo);
    return;
  }

  console.log("Uso:");
  console.log("  node prod_notifications_validation.js auth");
  console.log("  node prod_notifications_validation.js pending --tipo A --allow-pending");
  process.exitCode = 1;
}

main().catch((error) => {
  console.error(`Erro: ${error.message}`);
  process.exitCode = 1;
});
