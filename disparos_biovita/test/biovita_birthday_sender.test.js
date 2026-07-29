"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  normalizeName,
  normalizePhone,
  parseArguments,
  run,
  validateAndSelect,
} = require("../biovita_birthday_sender");

const DATE = "2026-07-29";

function record({ id, name, phone, date = DATE, type = "A" }) {
  return {
    CD_SMS: id,
    CD_PACIENTE: id,
    NM_PACIENTE: name,
    NU_CELULAR: phone,
    ID_TIPO: type,
    DT_INC_INF: `${date}T09:00:00.000Z`,
    BO_ENVIADO: false,
  };
}

function fixture(records) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "biovita-sender-"));
  const input = path.join(directory, "input.json");
  const template = path.join(directory, "message.txt");
  const blocklist = path.join(directory, "active.json");
  const dataDir = path.join(directory, "data");
  fs.writeFileSync(input, JSON.stringify({ success: true, count: records.length, data: records }), "utf8");
  fs.writeFileSync(template, "Ola, {{Nome}}!", "utf8");
  fs.writeFileSync(blocklist, JSON.stringify({ phones: ["(48) 98888-2222"] }), "utf8");
  return { directory, input, template, blocklist, dataDir };
}

function optionsFrom(files, overrides = {}) {
  return {
    input: files.input,
    fetchApi: false,
    send: true,
    allowDefaultSession: true,
    date: DATE,
    limit: 20,
    minDelayMs: 0,
    maxDelayMs: 0,
    messageFile: files.template,
    blocklist: files.blocklist,
    dataDir: files.dataDir,
    session: "default",
    wahaBaseUrl: "http://waha:3000",
    wahaApiKey: "test-key",
    ...overrides,
  };
}

test("normaliza o formato da API para chatId WAHA e recusa placeholders", () => {
  assert.deepEqual(normalizePhone("(48)988012556"), {
    ok: true,
    national: "48988012556",
    e164: "5548988012556",
    chatId: "5548988012556@c.us",
  });
  assert.equal(normalizePhone("(00)0000000").ok, false);
  assert.equal(normalizePhone("(11)11111111").ok, false);
  assert.equal(normalizePhone("(48)999999999").ok, false);
});

test("normaliza apenas o primeiro nome antes de montar a mensagem", () => {
  assert.equal(normalizeName("EDUARDO"), "Eduardo");
  assert.equal(normalizeName("  MARIA   EDUARDA DOS SANTOS "), "Maria");
  assert.equal(normalizeName("ANA-CLARA D'AVILA"), "Ana-Clara");
  assert.equal(normalizeName("JOS\u00c9 DA SILVA"), "Jos\u00e9");
});

test("usa BITLAB_API_KEY como chave da API de aniversariantes", () => {
  const options = parseArguments(["--fetch-api", "--message-file", "message.txt"], {
    BITLAB_API_KEY: "bitlab-key",
    BIOVITA_API_KEY: "old-key",
  });

  assert.equal(options.apiKey, "bitlab-key");
});

test("filtra tipo, data, duplicidade e conversa ativa antes de enviar", () => {
  const records = [
    record({ id: "1", name: "Ana", phone: "(48) 98888-1111" }),
    record({ id: "2", name: "Bloqueado", phone: "(48) 98888-2222" }),
    record({ id: "3", name: "Duplicado", phone: "(48) 98888-1111" }),
    record({ id: "4", name: "Data Errada", phone: "(48) 98888-3333", date: "2026-07-28" }),
    record({ id: "5", name: "Outro Tipo", phone: "(48) 98888-4444", type: "P" }),
  ];
  const result = validateAndSelect(records, {
    targetDate: DATE,
    blockedPhones: new Set(["5548988882222"]),
  });

  assert.equal(result.eligible.length, 1);
  assert.equal(result.eligible[0].phone.chatId, "5548988881111@c.us");
  assert.deepEqual(result.skipped.map((item) => item.reason).sort(), [
    "conversa_ativa",
    "data_referencia_diferente_do_dia",
    "telefone_duplicado_no_json",
    "tipo_nao_e_aniversario",
  ]);
});

test("envia somente numeros existentes no JSON e nunca repete no mesmo dia", async () => {
  const files = fixture([
    record({ id: "1", name: "ANA", phone: "(48) 98888-1111" }),
    record({ id: "2", name: "Em conversa", phone: "(48) 98888-2222" }),
    record({ id: "3", name: "BRUNO", phone: "(48) 98888-3333" }),
  ]);
  const requests = [];
  const fetchImpl = async (url, request = {}) => {
    const parsedUrl = new URL(String(url));
    if (parsedUrl.pathname === "/api/contacts/check-exists") {
      const phone = parsedUrl.searchParams.get("phone");
      return new Response(JSON.stringify({ numberExists: true, chatId: `${phone}@c.us` }), { status: 200 });
    }
    requests.push(JSON.parse(request.body));
    return new Response(JSON.stringify({ id: `message-${requests.length}` }), { status: 200 });
  };
  const options = optionsFrom(files);

  const first = await run(options, { fetchImpl, sleepImpl: async () => {} });
  const second = await run(options, { fetchImpl, sleepImpl: async () => {} });

  assert.equal(first.results.filter((item) => item.status === "sent").length, 2);
  assert.equal(second.results.length, 0);
  assert.equal(second.skipped.filter((item) => item.reason === "ja_tentado_neste_dia").length, 2);
  assert.deepEqual(requests.map((request) => request.chatId).sort(), [
    "5548988881111@c.us",
    "5548988883333@c.us",
  ]);
  assert.deepEqual(requests.map((request) => request.text).sort(), [
    "Ola, Ana!",
    "Ola, Bruno!",
  ]);
  assert.ok(requests.every((request) => request.session === "default"));
});

test("consulta o WAHA antes do envio e usa o chatId LID resolvido", async () => {
  const files = fixture([
    record({ id: "1", name: "Ana", phone: "(48) 98888-1111" }),
  ]);
  const requests = [];
  const fetchImpl = async (url, request = {}) => {
    const parsedUrl = new URL(String(url));
    if (parsedUrl.pathname === "/api/contacts/check-exists") {
      assert.equal(parsedUrl.searchParams.get("phone"), "5548988881111");
      assert.equal(parsedUrl.searchParams.get("session"), "default");
      return new Response(JSON.stringify({ numberExists: true, chatId: "123456789@lid" }), { status: 200 });
    }
    requests.push(JSON.parse(request.body));
    return new Response(JSON.stringify({ id: "message-1" }), { status: 200 });
  };

  const report = await run(optionsFrom(files), { fetchImpl, sleepImpl: async () => {} });

  assert.equal(report.results[0].status, "sent");
  assert.equal(requests.length, 1);
  assert.equal(requests[0].chatId, "123456789@lid");
});

test("nao grava ledger nem envia quando WAHA informa que o numero nao existe", async () => {
  const files = fixture([
    record({ id: "1", name: "Ana", phone: "(48) 98888-1111" }),
  ]);
  const fetchImpl = async (url) => {
    assert.match(String(url), /\/api\/contacts\/check-exists/);
    return new Response(JSON.stringify({ numberExists: false }), { status: 200 });
  };

  const report = await run(optionsFrom(files), { fetchImpl, sleepImpl: async () => {} });

  assert.equal(report.results.length, 0);
  assert.equal(report.skipped.filter((item) => item.reason === "nao_registrado_no_whatsapp").length, 1);
  assert.equal(fs.existsSync(path.join(files.dataDir, "ledger", `${DATE}.json`)), false);
});

test("dry-run nao grava idempotencia e respeita o limite", async () => {
  const files = fixture([
    record({ id: "1", name: "Ana", phone: "(48) 98888-1111" }),
    record({ id: "2", name: "Bruno", phone: "(48) 98888-3333" }),
  ]);
  const report = await run(optionsFrom(files, { send: false, limit: 1 }));
  assert.equal(report.results.length, 1);
  assert.equal(report.results[0].status, "dry-run");
  assert.equal(report.skipped.filter((item) => item.reason === "fora_do_limite").length, 1);
  assert.equal(fs.existsSync(path.join(files.dataDir, "ledger", `${DATE}.json`)), false);
});

test("permite validacao limitada mesmo que a API retorne mais registros quando ha aceite explicito", async () => {
  const files = fixture([]);
  const records = [
    record({ id: "1", name: "Ana", phone: "(48) 98888-1111" }),
    record({ id: "2", name: "Bruno", phone: "(48) 98888-3333" }),
  ];
  const requests = [];
  const fetchImpl = async (url, request = {}) => {
    if (String(url).includes("/api/contacts/check-exists")) {
      const phone = new URL(String(url)).searchParams.get("phone");
      return new Response(JSON.stringify({ numberExists: true, chatId: `${phone}@c.us` }), { status: 200 });
    }
    if (request.method === "POST") {
      requests.push(JSON.parse(request.body));
      return new Response(JSON.stringify({ id: "message-1" }), { status: 200 });
    }
    assert.match(String(url), /tipo=A/);
    assert.match(String(url), /limit=1/);
    return new Response(JSON.stringify({ success: true, count: 2, data: records }), { status: 200 });
  };
  const report = await run(optionsFrom(files, {
    input: "",
    fetchApi: true,
    limit: 1,
    apiKey: "api-key",
    apiLimitConfirmed: false,
    allowApiConsumeOverLimit: true,
  }), { fetchImpl, sleepImpl: async () => {} });

  assert.equal(report.sourceCount, 2);
  assert.equal(report.results.filter((item) => item.status === "sent").length, 1);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].chatId, "5548988881111@c.us");
});
