#!/usr/bin/env node

"use strict";

const fs = require("node:fs");
const path = require("node:path");

const DEFAULT_LIMIT = 20;
const DEFAULT_MIN_DELAY_MS = 15000;
const DEFAULT_MAX_DELAY_MS = 30000;
const API_URL = "https://integracoes.bitlab.net.br/webhook/notificacao_biovita";

// DDDs currently assigned in Brazil. Keeping the list explicit prevents obvious
// placeholder values such as 00 from reaching WAHA.
const BRAZILIAN_DDDS = new Set([
  "11", "12", "13", "14", "15", "16", "17", "18", "19",
  "21", "22", "24", "27", "28",
  "31", "32", "33", "34", "35", "37", "38",
  "41", "42", "43", "44", "45", "46", "47", "48", "49",
  "51", "53", "54", "55",
  "61", "62", "63", "64", "65", "66", "67", "68", "69",
  "71", "73", "74", "75", "77", "79",
  "81", "82", "83", "84", "85", "86", "87", "88", "89",
  "91", "92", "93", "94", "95", "96", "97", "98", "99",
]);

function fail(message) {
  const error = new Error(message);
  error.isUserError = true;
  throw error;
}

function describeNetworkError(error) {
  const details = [error?.message, error?.cause?.code, error?.cause?.message]
    .filter(Boolean)
    .map(String);
  return [...new Set(details)].join(" | ") || "erro de rede desconhecido";
}

function parseBoolean(value) {
  return ["1", "true", "yes", "sim"].includes(String(value || "").trim().toLowerCase());
}

function parsePositiveInteger(value, name, { allowZero = false } = {}) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0 || (!allowZero && parsed === 0)) {
    fail(`${name} deve ser um inteiro ${allowZero ? "maior ou igual a zero" : "maior que zero"}.`);
  }
  return parsed;
}

function getSaoPauloDate(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function isValidIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() + 1 === month && date.getUTCDate() === day;
}

function getDatePart(value) {
  const match = String(value || "").match(/^(\d{4}-\d{2}-\d{2})/);
  return match && isValidIsoDate(match[1]) ? match[1] : null;
}

function isRepeated(value) {
  return /^(\d)\1+$/.test(value);
}

function isSequential(value) {
  if (!/^\d+$/.test(value) || value.length < 7) return false;
  const digits = [...value].map(Number);
  const ascending = digits.every((digit, index) => index === 0 || digit === digits[index - 1] + 1);
  const descending = digits.every((digit, index) => index === 0 || digit === digits[index - 1] - 1);
  return ascending || descending;
}

function normalizePhone(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) return { ok: false, reason: "telefone_ausente" };

  // The API normally returns DDD + mobile number. Country code is accepted too
  // so a manual blocklist can use either format.
  const national = digits.startsWith("55") && [12, 13].includes(digits.length) ? digits.slice(2) : digits;

  if (national.length !== 11) return { ok: false, reason: "telefone_deve_ter_ddd_mais_9_digitos" };
  if (isRepeated(national)) return { ok: false, reason: "telefone_com_digitos_repetidos" };

  const ddd = national.slice(0, 2);
  const mobile = national.slice(2);
  if (!BRAZILIAN_DDDS.has(ddd)) return { ok: false, reason: "ddd_invalido" };
  if (!mobile.startsWith("9")) return { ok: false, reason: "telefone_nao_e_celular" };
  if (isRepeated(mobile) || isSequential(mobile)) {
    return { ok: false, reason: "telefone_com_padrao_placeholder" };
  }

  const e164 = `55${national}`;
  return { ok: true, national, e164, chatId: `${e164}@c.us` };
}

function maskPhone(value) {
  const normalized = normalizePhone(value);
  if (!normalized.ok) return "[invalido]";
  return `+55******${normalized.national.slice(-4)}`;
}

function titleCaseNamePart(value) {
  if (!value) return value;
  return value.charAt(0).toLocaleUpperCase("pt-BR") + value.slice(1).toLocaleLowerCase("pt-BR");
}

function normalizeName(value) {
  const cleaned = String(value || "").normalize("NFC").replace(/\s+/g, " ").trim();
  if (!cleaned) return "";
  const firstName = cleaned.split(" ")[0];

  return firstName
    .toLocaleLowerCase("pt-BR")
    .split("-")
    .map((hyphenPart) => hyphenPart.split("'").map(titleCaseNamePart).join("'"))
    .join("-");
}

function renderMessage(template, name) {
  if (!template.includes("{{Nome}}")) {
    fail("A mensagem deve conter exatamente o marcador {{Nome}}.");
  }
  const rendered = template.replaceAll("{{Nome}}", name);
  if (/{{[^}]+}}/.test(rendered)) {
    fail("A mensagem possui marcadores nao preenchidos.");
  }
  return rendered;
}

function readJson(filePath, label = "arquivo JSON") {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    fail(`Nao foi possivel ler ${label} em ${filePath}: ${error.message}`);
  }
}

function extractRecords(document) {
  const root = Array.isArray(document) ? document[0] : document;
  const candidates = [
    root,
    root?.payload,
    root?.calls?.sentAfterTransition?.response?.payload,
    root?.calls?.pendingAfterTransition?.response?.payload,
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate?.data)) return candidate.data;
  }
  fail("JSON sem uma lista reconhecida em data[]. Use o payload da API ou o log exportado.");
}

function loadBlockedPhones(filePath) {
  if (!filePath) return new Set();
  if (!fs.existsSync(filePath)) return new Set();

  const document = readJson(filePath, "lista de conversas ativas");
  const values = Array.isArray(document)
    ? document
    : document.phones || document.activeConversations || document.numbers || [];
  if (!Array.isArray(values)) fail("A lista de conversas ativas deve possuir phones: [].");

  const blocked = new Set();
  for (const entry of values) {
    const phone = typeof entry === "object" && entry !== null ? entry.phone || entry.number || entry.NU_CELULAR : entry;
    const normalized = normalizePhone(phone);
    if (normalized.ok) blocked.add(normalized.e164);
  }
  return blocked;
}

function validateAndSelect(records, { targetDate, blockedPhones }) {
  const eligible = [];
  const skipped = [];
  const seenPhones = new Set();

  records.forEach((record, index) => {
    const sourceIndex = index + 1;
    const sourceId = record?.CD_SMS ?? record?.CD_PACIENTE ?? `linha-${sourceIndex}`;
    const phone = normalizePhone(record?.NU_CELULAR);
    const name = normalizeName(record?.NM_PACIENTE);
    const recordDate = getDatePart(record?.DT_INC_INF);

    if (!record || typeof record !== "object") {
      skipped.push({ sourceIndex, sourceId, reason: "registro_invalido", phoneMasked: "[invalido]" });
      return;
    }
    if (record.ID_TIPO !== "A") {
      skipped.push({ sourceIndex, sourceId, reason: "tipo_nao_e_aniversario", phoneMasked: maskPhone(record.NU_CELULAR) });
      return;
    }
    if (!recordDate) {
      skipped.push({ sourceIndex, sourceId, reason: "data_referencia_invalida", phoneMasked: maskPhone(record.NU_CELULAR) });
      return;
    }
    if (recordDate !== targetDate) {
      skipped.push({ sourceIndex, sourceId, reason: "data_referencia_diferente_do_dia", phoneMasked: maskPhone(record.NU_CELULAR) });
      return;
    }
    if (!phone.ok) {
      skipped.push({ sourceIndex, sourceId, reason: phone.reason, phoneMasked: "[invalido]" });
      return;
    }
    if (!name) {
      skipped.push({ sourceIndex, sourceId, reason: "nome_ausente", phoneMasked: maskPhone(record.NU_CELULAR) });
      return;
    }
    if (seenPhones.has(phone.e164)) {
      skipped.push({ sourceIndex, sourceId, reason: "telefone_duplicado_no_json", phoneMasked: maskPhone(record.NU_CELULAR) });
      return;
    }
    seenPhones.add(phone.e164);
    if (blockedPhones.has(phone.e164)) {
      skipped.push({ sourceIndex, sourceId, reason: "conversa_ativa", phoneMasked: maskPhone(record.NU_CELULAR) });
      return;
    }

    eligible.push({
      sourceIndex,
      sourceId: String(sourceId),
      phone,
      name,
    });
  });

  return { eligible, skipped };
}

function randomDelay(minDelayMs, maxDelayMs, random = Math.random) {
  if (minDelayMs === maxDelayMs) return minDelayMs;
  return Math.floor(minDelayMs + random() * (maxDelayMs - minDelayMs + 1));
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function writeJsonAtomic(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
  fs.renameSync(temporaryPath, filePath);
}

function loadLedger(dataDir, targetDate) {
  const ledgerPath = path.join(dataDir, "ledger", `${targetDate}.json`);
  if (!fs.existsSync(ledgerPath)) {
    return { ledgerPath, ledger: { version: 1, date: targetDate, recipients: {} } };
  }
  const ledger = readJson(ledgerPath, "controle diario de envios");
  if (ledger?.date !== targetDate || !ledger?.recipients || typeof ledger.recipients !== "object") {
    fail(`Controle diario invalido: ${ledgerPath}`);
  }
  return { ledgerPath, ledger };
}

async function withDailyLock(dataDir, targetDate, callback) {
  const lockDir = path.join(dataDir, "locks");
  fs.mkdirSync(lockDir, { recursive: true, mode: 0o700 });
  const lockPath = path.join(lockDir, `${targetDate}.lock`);
  let descriptor;
  try {
    descriptor = fs.openSync(lockPath, "wx", 0o600);
    fs.writeFileSync(descriptor, `${process.pid}\n`);
  } catch (error) {
    if (error.code === "EEXIST") {
      fail(`Ja existe uma execucao em andamento para ${targetDate}. Aguarde-a terminar antes de rodar novamente.`);
    }
    throw error;
  }

  try {
    return await callback();
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    if (fs.existsSync(lockPath)) fs.unlinkSync(lockPath);
  }
}

async function sendTextWithWaha({ baseUrl, apiKey, session, chatId, text, fetchImpl = fetch }) {
  const endpoint = `${baseUrl.replace(/\/$/, "")}/api/sendText`;
  let response;
  try {
    response = await fetchImpl(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": apiKey,
      },
      body: JSON.stringify({ chatId, text, session }),
      signal: AbortSignal.timeout(30000),
    });
  } catch (error) {
    fail(`Falha ao conectar no WAHA (${baseUrl}): ${describeNetworkError(error)}`);
  }

  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`WAHA respondeu HTTP ${response.status}: ${responseText.slice(0, 300)}`);
  }

  try {
    const responseJson = JSON.parse(responseText);
    return String(responseJson?.id || responseJson?.messageId || responseJson?.key?.id || "");
  } catch {
    return "";
  }
}

function parseArguments(argv, env = process.env) {
  const args = [...argv];
  const options = {
    input: "",
    fetchApi: false,
    send: false,
    allowDefaultSession: false,
    date: getSaoPauloDate(),
    limit: parsePositiveInteger(env.BIOVITA_BATCH_LIMIT || DEFAULT_LIMIT, "BIOVITA_BATCH_LIMIT", { allowZero: true }),
    minDelayMs: parsePositiveInteger(env.BIOVITA_MIN_DELAY_MS || DEFAULT_MIN_DELAY_MS, "BIOVITA_MIN_DELAY_MS", { allowZero: true }),
    maxDelayMs: parsePositiveInteger(env.BIOVITA_MAX_DELAY_MS || DEFAULT_MAX_DELAY_MS, "BIOVITA_MAX_DELAY_MS", { allowZero: true }),
    messageFile: env.BIOVITA_MESSAGE_FILE || "",
    blocklist: env.BIOVITA_BLOCKLIST_FILE || "",
    dataDir: env.BIOVITA_DATA_DIR || path.join(process.cwd(), ".runtime"),
    session: env.BIOVITA_WAHA_SESSION || "biovita-disparos",
    wahaBaseUrl: env.BIOVITA_WAHA_BASE_URL || "http://waha:3000",
    wahaApiKey: env.BIOVITA_WAHA_API_KEY || env.WAHA_API_KEY || "",
    apiKey: env.BITLAB_API_KEY || env.BIOVITA_API_KEY || "",
    apiLimitParam: env.BIOVITA_API_LIMIT_PARAM || "limit",
    apiLimitConfirmed: parseBoolean(env.BIOVITA_API_LIMIT_CONFIRMED),
    allowApiConsumeOverLimit: false,
  };

  function takeValue(flag) {
    const value = args.shift();
    if (!value || value.startsWith("--")) fail(`${flag} exige um valor.`);
    return value;
  }

  while (args.length > 0) {
    const arg = args.shift();
    switch (arg) {
      case "--input": options.input = takeValue(arg); break;
      case "--fetch-api": options.fetchApi = true; break;
      case "--allow-api-consume-over-limit": options.allowApiConsumeOverLimit = true; break;
      case "--send": options.send = true; break;
      case "--allow-default-session": options.allowDefaultSession = true; break;
      case "--date": options.date = takeValue(arg); break;
      case "--limit": options.limit = parsePositiveInteger(takeValue(arg), arg, { allowZero: true }); break;
      case "--min-delay-ms": options.minDelayMs = parsePositiveInteger(takeValue(arg), arg, { allowZero: true }); break;
      case "--max-delay-ms": options.maxDelayMs = parsePositiveInteger(takeValue(arg), arg, { allowZero: true }); break;
      case "--message-file": options.messageFile = takeValue(arg); break;
      case "--blocklist": options.blocklist = takeValue(arg); break;
      case "--data-dir": options.dataDir = takeValue(arg); break;
      case "--session": options.session = takeValue(arg); break;
      case "--help": options.help = true; break;
      default: fail(`Argumento desconhecido: ${arg}`);
    }
  }

  if (!isValidIsoDate(options.date)) fail("--date deve usar o formato YYYY-MM-DD.");
  if (options.minDelayMs > options.maxDelayMs) fail("--min-delay-ms nao pode ser maior que --max-delay-ms.");
  if (options.input && options.fetchApi) fail("Use --input ou --fetch-api, nunca os dois juntos.");
  if (!options.input && !options.fetchApi && !options.help) fail("Informe --input ARQUIVO para teste ou --fetch-api para producao.");
  return options;
}

function usage() {
  return [
    "Uso:",
    "  node biovita_birthday_sender.js --input input.manual.json --message-file message.txt [opcoes]",
    "",
    "Opcoes principais:",
    "  --send                         Executa os envios. Sem esta flag, somente simula.",
    "  --limit N                      Maximo de destinatarios validos (padrao: 20; 0 = sem limite).",
    "  --date YYYY-MM-DD              Dia de referencia; padrao: hoje em America/Sao_Paulo.",
    "  --blocklist ARQUIVO            JSON com phones: [] de conversas a excluir.",
    "  --session NOME                 Sessao WAHA; default exige --allow-default-session.",
    "  --min-delay-ms N --max-delay-ms N",
    "  --fetch-api                    Bloqueado ate confirmar paginação/limit na API.",
    "  --allow-api-consume-over-limit Permite validacao mesmo se a API consumir mais registros que o limite.",
  ].join("\n");
}

async function requestApiPayload(options, fetchImpl = fetch) {
  if (!options.apiLimitConfirmed && !options.allowApiConsumeOverLimit) {
    fail("--fetch-api bloqueado: confirme antes que a API respeita o parametro de limite sem marcar registros extras como enviados. Configure BIOVITA_API_LIMIT_CONFIRMED=true somente apos essa confirmacao.");
  }
  if (!options.apiKey) fail("BITLAB_API_KEY e obrigatoria com --fetch-api.");
  if (options.limit === 0) fail("--fetch-api nao aceita --limit 0.");

  const url = new URL(API_URL);
  url.searchParams.set("tipo", "A");
  url.searchParams.set("status", "pendente");
  const apiLimitParam = options.apiLimitParam || "limit";
  url.searchParams.set(apiLimitParam, String(options.limit));
  let response;
  try {
    response = await fetchImpl(url, {
      headers: { "X-API-KEY": options.apiKey, Accept: "application/json" },
      signal: AbortSignal.timeout(30000),
    });
  } catch (error) {
    fail(`Falha ao conectar na API Bitlab (${url.hostname}): ${describeNetworkError(error)}`);
  }

  const text = await response.text();
  if (!response.ok) fail(`API Biovita respondeu HTTP ${response.status}: ${text.slice(0, 300)}`);
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    fail("API Biovita nao retornou JSON.");
  }
  const records = extractRecords(payload);
  if (records.length > options.limit && !options.allowApiConsumeOverLimit) {
    fail(`A API retornou ${records.length} registros para limite ${options.limit}. O parametro ${apiLimitParam} nao foi respeitado; nenhum envio sera feito.`);
  }
  return payload;
}

function readTemplate(messageFile) {
  if (!messageFile) fail("Informe --message-file ou BIOVITA_MESSAGE_FILE.");
  try {
    const template = fs.readFileSync(messageFile, "utf8").trim();
    if (!template) fail("O arquivo da mensagem esta vazio.");
    return template;
  } catch (error) {
    if (error.isUserError) throw error;
    fail(`Nao foi possivel ler a mensagem em ${messageFile}: ${error.message}`);
  }
}

function recipientResult(recipient, status, reason, extra = {}) {
  return {
    sourceIndex: recipient.sourceIndex,
    sourceId: recipient.sourceId,
    phoneMasked: `+55******${recipient.phone.national.slice(-4)}`,
    status,
    reason,
    ...extra,
  };
}

function writeReport(dataDir, report) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const reportPath = path.join(dataDir, "reports", `${report.targetDate}_${timestamp}.json`);
  writeJsonAtomic(reportPath, report);
  return reportPath;
}

async function run(options, dependencies = {}) {
  const fetchImpl = dependencies.fetchImpl || fetch;
  const sleepImpl = dependencies.sleepImpl || sleep;
  const random = dependencies.random || Math.random;
  const template = readTemplate(options.messageFile);
  const sourceDocument = options.fetchApi
    ? await requestApiPayload(options, fetchImpl)
    : readJson(options.input, "JSON de entrada");
  const records = extractRecords(sourceDocument);
  const blockedPhones = loadBlockedPhones(options.blocklist);
  const prepared = validateAndSelect(records, { targetDate: options.date, blockedPhones });
  const limitedRecipients = options.limit === 0 ? prepared.eligible : prepared.eligible.slice(0, options.limit);
  const overLimit = options.limit === 0 ? [] : prepared.eligible.slice(options.limit);
  const report = {
    generatedAt: new Date().toISOString(),
    targetDate: options.date,
    mode: options.send ? "send" : "dry-run",
    session: options.session,
    sourceCount: records.length,
    blockedPhoneCount: blockedPhones.size,
    requestedLimit: options.limit,
    selectedCount: limitedRecipients.length,
    skipped: [...prepared.skipped, ...overLimit.map((recipient) => recipientResult(recipient, "skipped", "fora_do_limite"))],
    results: [],
  };

  for (const recipient of limitedRecipients) {
    // Validate the rendered output before touching the ledger or WAHA.
    renderMessage(template, recipient.name);
  }

  if (!options.send) {
    for (const recipient of limitedRecipients) {
      report.results.push(recipientResult(recipient, "dry-run", "pronto_para_envio"));
    }
    report.reportPath = writeReport(options.dataDir, report);
    return report;
  }

  if (options.session === "default" && !options.allowDefaultSession) {
    fail("A sessao default so pode ser usada com --allow-default-session.");
  }
  if (!options.wahaBaseUrl) fail("BIOVITA_WAHA_BASE_URL e obrigatoria para enviar.");
  if (!options.wahaApiKey) fail("BIOVITA_WAHA_API_KEY e obrigatoria para enviar.");

  await withDailyLock(options.dataDir, options.date, async () => {
    const { ledgerPath, ledger } = loadLedger(options.dataDir, options.date);
    const candidates = [];
    for (const recipient of limitedRecipients) {
      if (ledger.recipients[recipient.phone.e164]) {
        report.skipped.push(recipientResult(recipient, "skipped", "ja_tentado_neste_dia"));
      } else {
        candidates.push(recipient);
      }
    }

    for (let index = 0; index < candidates.length; index += 1) {
      const recipient = candidates[index];
      const text = renderMessage(template, recipient.name);
      const attemptedAt = new Date().toISOString();

      // Persist before the request. A timeout may still have delivered the message,
      // so a later execution must not retry it automatically and duplicate a wish.
      ledger.recipients[recipient.phone.e164] = {
        status: "attempting",
        sourceId: recipient.sourceId,
        attemptedAt,
      };
      writeJsonAtomic(ledgerPath, ledger);

      try {
        const messageId = await sendTextWithWaha({
          baseUrl: options.wahaBaseUrl,
          apiKey: options.wahaApiKey,
          session: options.session,
          chatId: recipient.phone.chatId,
          text,
          fetchImpl,
        });
        ledger.recipients[recipient.phone.e164] = {
          ...ledger.recipients[recipient.phone.e164],
          status: "sent",
          completedAt: new Date().toISOString(),
          messageId: messageId || undefined,
        };
        writeJsonAtomic(ledgerPath, ledger);
        report.results.push(recipientResult(recipient, "sent", "enviado"));
      } catch (error) {
        ledger.recipients[recipient.phone.e164] = {
          ...ledger.recipients[recipient.phone.e164],
          status: "uncertain",
          completedAt: new Date().toISOString(),
          error: String(error.message || error).slice(0, 300),
        };
        writeJsonAtomic(ledgerPath, ledger);
        report.results.push(recipientResult(recipient, "uncertain", "falha_sem_retentativa_automatica"));
      }

      if (index < candidates.length - 1) {
        await sleepImpl(randomDelay(options.minDelayMs, options.maxDelayMs, random));
      }
    }
  });

  report.reportPath = writeReport(options.dataDir, report);
  return report;
}

function printSummary(report) {
  const sent = report.results.filter((item) => item.status === "sent").length;
  const dryRun = report.results.filter((item) => item.status === "dry-run").length;
  const uncertain = report.results.filter((item) => item.status === "uncertain").length;
  console.log(`data: ${report.targetDate}`);
  console.log(`modo: ${report.mode}`);
  console.log(`registros de origem: ${report.sourceCount}`);
  console.log(`selecionados: ${report.selectedCount}`);
  console.log(`enviados: ${sent}`);
  console.log(`simulados: ${dryRun}`);
  console.log(`incertos: ${uncertain}`);
  console.log(`ignorados: ${report.skipped.length}`);
  console.log(`relatorio: ${report.reportPath}`);
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  if (options.help) {
    console.log(usage());
    return;
  }
  const report = await run(options);
  printSummary(report);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(`Erro: ${error.message}`);
    process.exitCode = 1;
  });
}

module.exports = {
  extractRecords,
  getDatePart,
  getSaoPauloDate,
  isValidIsoDate,
  loadBlockedPhones,
  normalizeName,
  normalizePhone,
  parseArguments,
  renderMessage,
  run,
  validateAndSelect,
};
