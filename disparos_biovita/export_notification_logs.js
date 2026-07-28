const fs = require("fs");
const path = require("path");

const BASE_URL = "https://integracoes.bitlab.net.br/webhook/notificacao_biovita";

function getApiKey() {
  return process.env.BITLAB_API_KEY || process.env.BIOVITA_API_KEY || null;
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

  const payload = Array.isArray(json) ? json[0] : json;

  return {
    requestedAt: new Date().toISOString(),
    url,
    httpStatus: response.status,
    rawText: text,
    payload,
  };
}

function summarize(payload) {
  const data = Array.isArray(payload?.data) ? payload.data : [];
  const tipoCounts = {};
  const dates = new Set();

  for (const item of data) {
    tipoCounts[item.ID_TIPO] = (tipoCounts[item.ID_TIPO] || 0) + 1;
    if (item.DT_INC_INF) dates.add(String(item.DT_INC_INF).substring(0, 10));
  }

  return {
    success: payload?.success ?? null,
    count: payload?.count ?? null,
    message: payload?.message ?? null,
    tipoCounts,
    dates: [...dates].sort(),
  };
}

async function main() {
  const apiKey = getApiKey();
  if (!apiKey) throw new Error("Informe BITLAB_API_KEY no ambiente.");

  const startedAt = new Date().toISOString();
  const pending = await requestNotifications({ apiKey, tipo: "A", status: "pendente" });
  const sent = await requestNotifications({ apiKey, tipo: "A", status: "enviado" });

  const log = {
    generatedAt: new Date().toISOString(),
    note:
      "A chamada original status=pendente executada antes deste arquivo retornou 630 aniversariantes de 2026-07-22 e marcou os registros como enviados. Este arquivo salva a fotografia posterior: pendente zerado e enviado com os aniversariantes.",
    originalValidationObserved: {
      startedAt,
      tipo: "A",
      firstPendingCall: {
        httpStatus: 200,
        success: true,
        count: 630,
        dates: ["2026-07-22"],
        transitionEffect: "registros deixaram status=pendente apos a consulta",
      },
      secondPendingCall: {
        httpStatus: 200,
        success: true,
        count: 0,
        message: "Nenhuma notificacao disponivel para os filtros informados.",
      },
    },
    calls: {
      pendingAfterTransition: {
        summary: summarize(pending.payload),
        response: pending,
      },
      sentAfterTransition: {
        summary: summarize(sent.payload),
        response: sent,
      },
    },
  };

  const filename = `notification_validation_A_${new Date()
    .toISOString()
    .replace(/[:.]/g, "-")}.json`;
  const outputPath = path.resolve(filename);

  fs.writeFileSync(outputPath, JSON.stringify(log, null, 2), "utf8");

  console.log(outputPath);
  console.log(`pendingAfterTransition count: ${log.calls.pendingAfterTransition.summary.count}`);
  console.log(`sentAfterTransition count: ${log.calls.sentAfterTransition.summary.count}`);
}

main().catch((error) => {
  console.error(`Erro: ${error.message}`);
  process.exitCode = 1;
});
