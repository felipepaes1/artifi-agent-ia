const API_KEY = process.env.BITLAB_API_KEY || process.env.BIOVITA_API_KEY;
const BASE_URL = "https://integracoes.bitlab.net.br/webhook/notificacao_biovita";

const tests = [
  { name: "Todos pendentes", url: `${BASE_URL}?tipo=*&status=pendente` },
  { name: "Todos enviados", url: `${BASE_URL}?tipo=*&status=enviado` },
  { name: "Sem filtros", url: `${BASE_URL}` },
  { name: "Paciente novo P", url: `${BASE_URL}?tipo=P&status=pendente` },
  { name: "Resultado disponível R", url: `${BASE_URL}?tipo=R&status=pendente` },
  { name: "Aniversariantes A", url: `${BASE_URL}?tipo=A&status=pendente` },
  { name: "Tipo não documentado N", url: `${BASE_URL}?tipo=N&status=pendente` },
  { name: "Múltiplos tipos documentados", url: `${BASE_URL}?tipo=P,R,A&status=pendente` },
  { name: "Múltiplos tipos com N", url: `${BASE_URL}?tipo=N,P,R,A&status=pendente` },
  { name: "Status inválido", url: `${BASE_URL}?tipo=*&status=teste` },
  { name: "Tipo inválido", url: `${BASE_URL}?tipo=XYZ&status=pendente` },
];

async function runTests() {
  if (!API_KEY) throw new Error("Informe BITLAB_API_KEY no ambiente.");

  for (const test of tests) {
    console.log("\n==============================");
    console.log("Teste:", test.name);
    console.log("URL:", test.url);

    try {
      const response = await fetch(test.url, {
        method: "GET",
        headers: {
          "x-api-key": API_KEY,
          "Accept": "application/json"
        },
        signal: AbortSignal.timeout(30000)
      });

      const text = await response.text();

      let json;
      try {
        json = JSON.parse(text);
      } catch {
        json = null;
      }

      console.log("HTTP Status:", response.status);

      if (!json) {
        console.log("Resposta não JSON:", text.slice(0, 500));
        continue;
      }

      const payload = Array.isArray(json) ? json[0] : json;

      console.log("success:", payload.success);
      console.log("count:", payload.count);
      console.log("message:", payload.message || null);

      if (Array.isArray(payload.data)) {
        const tipos = {};
        const datas = [];

        for (const item of payload.data) {
          tipos[item.ID_TIPO] = (tipos[item.ID_TIPO] || 0) + 1;

          if (item.DT_INC_INF) {
            datas.push(item.DT_INC_INF.substring(0, 10));
          }
        }

        const datasUnicas = [...new Set(datas)].sort();

        console.log("Tipos encontrados:", tipos);
        console.log("Datas encontradas:", datasUnicas);
        console.log("Primeiro item:", payload.data[0] || null);
      }
    } catch (error) {
      console.error("Erro no teste:", error.message);
    }
  }
}

runTests();
