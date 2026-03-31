const API = "";

/* ===== STATE ===== */
let pipelineData = null;
let explorerState = { subreddit: "", date: "", page: 1 };

/* ===== NAV ===== */
document.querySelectorAll(".nav-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".nav-btn").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".section").forEach((s) => s.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("section-" + btn.dataset.section).classList.add("active");
  });
});

/* ===== LAYER TABS ===== */
document.querySelectorAll(".layer-tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".layer-tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach((c) => c.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById("tab-" + tab.dataset.tab).classList.add("active");
  });
});

/* ===== PIPELINE ===== */
async function loadPipeline() {
  try {
    const res = await fetch(API + "/api/v1/pipeline/status");
    pipelineData = await res.json();
  } catch {
    try {
      const fallback = await fetch("/data.json");
      pipelineData = await fallback.json();
    } catch (e2) {
      console.error("Erro ao carregar pipeline:", e2);
      return;
    }
  }
  renderPipelineCounts();
  renderSilverTable(pipelineData.silver_posts);
  renderGold(pipelineData.gold);
}

function renderPipelineCounts() {
  const layers = pipelineData.layers;
  document.getElementById("bronze-records").textContent = layers[0].records.toLocaleString() + " registros";
  document.getElementById("silver-records").textContent = layers[1].records.toLocaleString() + " registros";
  document.getElementById("gold-records").textContent = layers[2].records.toLocaleString() + " registros";

  const silverBadge = document.getElementById("silver-badge");
  const goldBadge = document.getElementById("gold-badge");
  if (layers[1].status === "active" && silverBadge) {
    silverBadge.textContent = "ATIVO";
    silverBadge.className = "node-status badge-active";
  }
  if (layers[2].status === "active" && goldBadge) {
    goldBadge.textContent = "ATIVO";
    goldBadge.className = "node-status badge-active";
  }
}

document.querySelectorAll(".pipeline-node[data-layer]").forEach((node) => {
  node.addEventListener("click", () => {
    document.querySelectorAll(".pipeline-node").forEach((n) => n.classList.remove("selected"));
    node.classList.add("selected");
    showLayerDetail(parseInt(node.dataset.layer));
  });
});

function showLayerDetail(idx) {
  if (!pipelineData) return;
  const layer = pipelineData.layers[idx];
  const el = document.getElementById("layer-detail");

  const sampleJson = layer.sample
    ? JSON.stringify(layer.sample, null, 2).substring(0, 800)
    : "Sem dados disponíveis";

  el.innerHTML = `
    <h3>${layer.name} — ${layer.description}</h3>
    <div class="detail-grid">
      <div class="detail-meta">
        <p><strong>Status:</strong> ${layer.status === "active" ? "Ativo" : "Mock (preview)"}</p>
        <p><strong>Tecnologia:</strong> ${layer.tech}</p>
        <p><strong>Registros:</strong> ${layer.records.toLocaleString()}</p>
        ${layer.subreddits ? `<p><strong>Subreddits:</strong> ${layer.subreddits}</p>` : ""}
        ${layer.tools_detected ? `<p><strong>Ferramentas detectadas:</strong> ${layer.tools_detected}</p>` : ""}
      </div>
      <div class="detail-sample">
        <pre>${escapeHtml(sampleJson)}</pre>
      </div>
    </div>
  `;
}

/* ===== EXPLORER ===== */
async function loadSubreddits() {
  try {
    const res = await fetch(API + "/api/v1/bronze/subreddits");
    const data = await res.json();
    const sel = document.getElementById("sub-select");
    sel.innerHTML = data.map((s) => `<option value="${s.subreddit}">${s.subreddit} (${s.total_posts})</option>`).join("");
    if (data.length > 0) {
      explorerState.subreddit = data[0].subreddit;
      updateDates(data);
    }
    sel.addEventListener("change", () => {
      explorerState.subreddit = sel.value;
      updateDates(data);
    });
  } catch (e) {
    console.error("Erro ao carregar subreddits:", e);
  }
}

function updateDates(allSubs) {
  const sub = allSubs.find((s) => s.subreddit === explorerState.subreddit);
  const sel = document.getElementById("date-select");
  if (!sub) return;
  sel.innerHTML = sub.dates.map((d) => `<option value="${d.date}">${d.date} (${d.count} posts)</option>`).join("");
  explorerState.date = sub.dates[0]?.date || "";
  sel.addEventListener("change", () => { explorerState.date = sel.value; });
}

document.getElementById("btn-load").addEventListener("click", () => {
  explorerState.page = 1;
  loadPosts();
  loadStats();
});

async function loadPosts() {
  const { subreddit, date, page } = explorerState;
  const sort = document.getElementById("sort-select").value;
  try {
    const res = await fetch(API + `/api/v1/bronze/${subreddit}/${date}?page=${page}&per_page=20&sort_by=${sort}`);
    const data = await res.json();
    renderPostsTable(data.posts);
    renderPagination(data.page, data.pages);
  } catch (e) {
    console.error("Erro ao carregar posts:", e);
  }
}

async function loadStats() {
  const { subreddit, date } = explorerState;
  try {
    const res = await fetch(API + `/api/v1/bronze/${subreddit}/${date}/stats`);
    const data = await res.json();
    renderStats(data);
  } catch (e) {
    console.error("Erro ao carregar stats:", e);
  }
}

function renderStats(s) {
  document.getElementById("stats-row").innerHTML = `
    <div class="stat-card"><div class="stat-value">${s.total}</div><div class="stat-label">Posts</div></div>
    <div class="stat-card"><div class="stat-value">${s.score_avg}</div><div class="stat-label">Score Médio</div></div>
    <div class="stat-card"><div class="stat-value">${s.score_max}</div><div class="stat-label">Score Máximo</div></div>
    <div class="stat-card"><div class="stat-value">${s.comments_total?.toLocaleString()}</div><div class="stat-label">Total Comentários</div></div>
    <div class="stat-card"><div class="stat-value">${s.comments_avg}</div><div class="stat-label">Comentários/Post</div></div>
  `;
}

function renderPostsTable(posts) {
  document.getElementById("posts-body").innerHTML = posts
    .map(
      (p) => `
    <tr>
      <td title="${escapeHtml(p.title)}">${escapeHtml(truncate(p.title, 70))}</td>
      <td>${escapeHtml(p.author || "—")}</td>
      <td>${p.score}</td>
      <td>${p.num_comments}</td>
      <td>${p.flair ? `<span class="flair-tag">${escapeHtml(p.flair)}</span>` : "—"}</td>
      <td>${formatDate(p.created_date)}</td>
    </tr>`
    )
    .join("");
}

function renderPagination(current, total) {
  if (total <= 1) { document.getElementById("pagination").innerHTML = ""; return; }
  let html = "";
  const start = Math.max(1, current - 3);
  const end = Math.min(total, current + 3);
  if (current > 1) html += `<button class="page-btn" data-page="${current - 1}">←</button>`;
  for (let i = start; i <= end; i++) {
    html += `<button class="page-btn ${i === current ? "active" : ""}" data-page="${i}">${i}</button>`;
  }
  if (current < total) html += `<button class="page-btn" data-page="${current + 1}">→</button>`;
  const el = document.getElementById("pagination");
  el.innerHTML = html;
  el.querySelectorAll(".page-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      explorerState.page = parseInt(btn.dataset.page);
      loadPosts();
    });
  });
}

/* ===== SILVER TABLE ===== */
function renderSilverTable(posts) {
  document.getElementById("silver-body").innerHTML = posts
    .map(
      (p) => {
        const tools = p.tools_mentioned || [];
        const toolsCell = tools.length
          ? tools.map((t) => `<span class="tool-tag">${escapeHtml(t)}</span>`).join(" ")
          : '<span style="color:var(--text-dim)">—</span>';
        return `
    <tr>
      <td title="${escapeHtml(p.title)}">${escapeHtml(truncate(p.title, 55))}</td>
      <td>${escapeHtml(p.subreddit || "—")}</td>
      <td>${p.score}</td>
      <td>${toolsCell}</td>
      <td>${p.flair ? `<span class="flair-tag">${escapeHtml(p.flair)}</span>` : "—"}</td>
    </tr>`;
      }
    )
    .join("");
}

/* ===== GOLD ===== */
function renderGold(gold) {
  const grid = document.getElementById("gold-grid");
  const s = gold.summary || {};

  const summaryItems = [
    { value: (s.total_posts || 0).toLocaleString(), label: "Posts analisados" },
    { value: (s.unique_subreddits || 0).toLocaleString(), label: "Subreddits" },
    { value: s.avg_score || 0, label: "Score medio" },
    { value: (s.total_comments || 0).toLocaleString(), label: "Comentarios extraidos" },
    { value: (s.total_unique_commenters || 0).toLocaleString(), label: "Comentaristas unicos" },
  ];

  const summaryHtml = `
    <div class="gold-card">
      <h3>Resumo Geral</h3>
      <div class="summary-grid">
        ${summaryItems.map((item) => `
          <div class="summary-item"><div class="s-value">${item.value}</div><div class="s-label">${item.label}</div></div>
        `).join("")}
      </div>
    </div>`;

  const rankings = gold.subreddit_rankings || [];
  const subsHtml = rankings.length ? `
    <div class="gold-card">
      <h3>Ranking de Subreddits</h3>
      ${rankings.map(
        (sub, i) => `
        <div class="ranking-item">
          <span class="ranking-pos">${i + 1}.</span>
          <span class="ranking-name">r/${escapeHtml(sub.subreddit)}</span>
          <span class="ranking-value">${(sub.posts || sub.post_count || 0).toLocaleString()} posts · avg ${sub.avg_score}</span>
        </div>`
      ).join("")}
    </div>` : "";

  const commenters = gold.top_commenters || [];
  const commentersHtml = commenters.length ? `
    <div class="gold-card">
      <h3>Top Comentaristas</h3>
      ${commenters.map(
        (c, i) => `
        <div class="ranking-item">
          <span class="ranking-pos">${i + 1}.</span>
          <span class="ranking-name">${escapeHtml(c.author)}</span>
          <span class="ranking-value">${(c.comment_count || 0).toLocaleString()} comentarios · avg ${(c.avg_score || 0).toFixed(1)}</span>
        </div>`
      ).join("")}
    </div>` : "";

  const tools = gold.tool_rankings || [];
  const toolsHtml = tools.length ? `
    <div class="gold-card">
      <h3>Top Ferramentas Mencionadas</h3>
      ${tools.map(
        (t, i) => `
        <div class="ranking-item">
          <span class="ranking-pos">${i + 1}.</span>
          <span class="ranking-name">${escapeHtml(t.tool)}</span>
          <span class="ranking-value">${t.mentions} mencoes</span>
        </div>`
      ).join("")}
    </div>` : "";

  grid.innerHTML = summaryHtml + subsHtml + commentersHtml + toolsHtml;
}

/* ===== UTILS ===== */
function escapeHtml(str) {
  if (!str) return "";
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function truncate(str, len) {
  if (!str) return "";
  return str.length > len ? str.substring(0, len) + "…" : str;
}
function formatDate(iso) {
  if (!iso) return "—";
  try { return new Date(iso).toLocaleDateString("pt-BR"); } catch { return iso; }
}

/* ===== INGEST ===== */
let _pollTimer = null;

document.getElementById("btn-ingest").addEventListener("click", triggerIngest);
document.getElementById("ingest-subs").addEventListener("keydown", (e) => {
  if (e.key === "Enter") triggerIngest();
});

async function triggerIngest() {
  const raw = document.getElementById("ingest-subs").value.trim();
  if (!raw) { showIngestMsg("Digite pelo menos um subreddit.", "error"); return; }

  const subreddits = raw.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
  const sort = document.getElementById("ingest-sort").value;
  const max_pages = parseInt(document.getElementById("ingest-pages").value);
  const upload_s3 = document.getElementById("ingest-s3").checked;

  document.getElementById("btn-ingest").disabled = true;
  const s3Label = upload_s3 ? " + upload S3" : "";
  showIngestMsg(`Disparando extração de r/${subreddits.join(", r/")}${s3Label}...`, "loading");
  renderTracker(null);

  try {
    const res = await fetch(API + "/api/v1/ingest/trigger", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ subreddits, sort, max_pages, upload_s3 }),
    });
    const data = await res.json();

    if (res.ok) {
      showIngestMsg(
        `Pipeline disparado! Acompanhando progresso...`,
        "loading"
      );
      startPolling(data.dag_run_id);
    } else {
      showIngestMsg(`Erro ${res.status}: ${JSON.stringify(data.detail || data, null, 2)}`, "error");
      document.getElementById("btn-ingest").disabled = false;
    }
  } catch (e) {
    showIngestMsg(`Erro de conexão: ${e.message}`, "error");
    document.getElementById("btn-ingest").disabled = false;
  }
}

function startPolling(dagRunId) {
  if (_pollTimer) clearInterval(_pollTimer);
  pollProgress(dagRunId);
  _pollTimer = setInterval(() => pollProgress(dagRunId), 5000);
}

async function pollProgress(dagRunId) {
  try {
    const res = await fetch(API + `/api/v1/ingest/dag-run/${encodeURIComponent(dagRunId)}`);
    const data = await res.json();

    renderTracker(data);

    if (data.state === "success") {
      clearInterval(_pollTimer);
      _pollTimer = null;
      const elapsed = data.start_date && data.end_date
        ? Math.round((new Date(data.end_date) - new Date(data.start_date)) / 1000)
        : null;
      showIngestMsg(
        `Pipeline concluído com sucesso!` +
        (elapsed ? ` (${elapsed}s)` : ""),
        "success"
      );
      document.getElementById("btn-ingest").disabled = false;
      loadSubreddits();
      loadPipeline();
    } else if (data.state === "failed") {
      clearInterval(_pollTimer);
      _pollTimer = null;
      const failedStep = data.steps.find((s) => s.state === "failed");
      showIngestMsg(
        `Pipeline falhou na etapa: ${failedStep ? failedStep.label : "desconhecida"}`,
        "error"
      );
      document.getElementById("btn-ingest").disabled = false;
    } else {
      const running = data.steps.find((s) => s.state === "running");
      showIngestMsg(
        running ? `Executando: ${running.label}...` : "Aguardando início...",
        "loading"
      );
    }
  } catch {
    showIngestMsg("Verificando progresso...", "loading");
  }
}

const STEP_ICONS = {
  success: "✓",
  running: "⟳",
  failed: "✗",
  upstream_failed: "⊘",
  queued: "·",
  scheduled: "·",
  no_status: "·",
};

const STEP_CLASSES = {
  success: "step-success",
  running: "step-running",
  failed: "step-failed",
  upstream_failed: "step-failed",
  queued: "step-queued",
  scheduled: "step-queued",
  no_status: "step-queued",
};

function renderTracker(data) {
  const el = document.getElementById("progress-tracker");
  if (!data || !data.steps) {
    el.innerHTML = "";
    return;
  }

  el.innerHTML = `
    <div class="tracker">
      ${data.steps.map((s, i) => `
        <div class="tracker-step ${STEP_CLASSES[s.state] || "step-queued"}">
          <div class="step-icon">${STEP_ICONS[s.state] || "·"}</div>
          <div class="step-info">
            <div class="step-label">${escapeHtml(s.label)}</div>
            ${s.count > 1 ? `<div class="step-count">${s.count} instâncias</div>` : ""}
          </div>
        </div>
        ${i < data.steps.length - 1 ? '<div class="tracker-connector"></div>' : ""}
      `).join("")}
    </div>
    <div class="tracker-state">Estado geral: <strong>${data.state}</strong></div>
  `;
}

function showIngestMsg(text, type) {
  document.getElementById("ingest-status").innerHTML =
    `<div class="ingest-msg ${type}"><pre>${escapeHtml(text)}</pre></div>`;
}

async function checkAirflow() {
  const el = document.getElementById("airflow-status");
  try {
    const res = await fetch(API + "/api/v1/ingest/dag-status");
    const data = await res.json();
    if (data.airflow === "online") {
      const paused = data.is_paused;
      el.innerHTML = `<span class="dot ${paused ? "dot-yellow" : "dot-green"}"></span>` +
        `Airflow: online · DAG ${data.dag_id}: ${paused ? "pausada" : "ativa"}`;
    } else {
      el.innerHTML = `<span class="dot dot-red"></span>Airflow: offline`;
    }
  } catch {
    el.innerHTML = `<span class="dot dot-red"></span>Airflow: não acessível`;
  }
}

/* ===== SCHEDULED ===== */
let _scheduledSubs = [];

async function loadScheduledStatus() {
  const statusLabel = document.getElementById("sched-status-label");
  const toggleBtn = document.getElementById("btn-toggle-sched");

  try {
    const res = await fetch(API + "/api/v1/ingest/scheduled/status");
    const data = await res.json();

    _scheduledSubs = data.subreddits || [];
    renderScheduledSubs();
    renderScheduledRuns(data.last_runs || []);

    if (data.is_paused) {
      statusLabel.textContent = "Pausada";
      toggleBtn.textContent = "Ativar";
      toggleBtn.className = "btn-toggle paused";
    } else {
      statusLabel.textContent = `Ativa · ${data.schedule || "a cada hora"}`;
      toggleBtn.textContent = "Pausar";
      toggleBtn.className = "btn-toggle active";
    }
  } catch {
    statusLabel.textContent = "Erro ao carregar";
    toggleBtn.textContent = "—";
  }
}

function renderScheduledSubs() {
  const el = document.getElementById("sched-subs-list");
  if (!_scheduledSubs.length) {
    el.innerHTML = '<span style="color:var(--text-dim);font-size:0.85rem">Nenhum subreddit cadastrado</span>';
    return;
  }
  el.innerHTML = _scheduledSubs.map(
    (s) => `<span class="sched-sub-tag">r/${escapeHtml(s)}<span class="remove-sub" data-sub="${escapeHtml(s)}">×</span></span>`
  ).join("");

  el.querySelectorAll(".remove-sub").forEach((btn) => {
    btn.addEventListener("click", () => removeScheduledSub(btn.dataset.sub));
  });
}

function renderScheduledRuns(runs) {
  const el = document.getElementById("sched-runs-list");
  if (!runs.length) {
    el.innerHTML = '<span style="color:var(--text-dim);font-size:0.85rem">Nenhuma execução ainda</span>';
    return;
  }
  el.innerHTML = runs.map((r) => {
    const stateClass = r.state === "success" ? "state-success"
      : r.state === "failed" ? "state-failed"
      : r.state === "running" ? "state-running"
      : "state-queued";
    const date = r.start_date ? new Date(r.start_date).toLocaleString("pt-BR") : "—";
    return `
      <div class="sched-run-item">
        <span>${date}</span>
        <span class="run-state ${stateClass}">${r.state}</span>
      </div>`;
  }).join("");
}

async function updateScheduledSubs(newList) {
  try {
    const res = await fetch(API + "/api/v1/ingest/scheduled/subreddits", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ subreddits: newList }),
    });
    if (res.ok) {
      _scheduledSubs = newList;
      renderScheduledSubs();
      showSchedMsg(`Subreddits atualizados: ${newList.join(", ")}`, "success");
    } else {
      const err = await res.json();
      showSchedMsg(`Erro: ${JSON.stringify(err.detail || err)}`, "error");
    }
  } catch (e) {
    showSchedMsg(`Erro de conexão: ${e.message}`, "error");
  }
}

function removeScheduledSub(sub) {
  const newList = _scheduledSubs.filter((s) => s !== sub);
  if (newList.length === 0) {
    showSchedMsg("Mantenha pelo menos 1 subreddit.", "error");
    return;
  }
  updateScheduledSubs(newList);
}

document.getElementById("btn-add-sub").addEventListener("click", addScheduledSub);
document.getElementById("sched-new-sub").addEventListener("keydown", (e) => {
  if (e.key === "Enter") addScheduledSub();
});

function addScheduledSub() {
  const input = document.getElementById("sched-new-sub");
  const val = input.value.trim().toLowerCase();
  if (!val) return;
  if (_scheduledSubs.includes(val)) {
    showSchedMsg(`r/${val} já está na lista.`, "error");
    return;
  }
  const newList = [..._scheduledSubs, val];
  updateScheduledSubs(newList);
  input.value = "";
}

document.getElementById("btn-toggle-sched").addEventListener("click", async () => {
  const btn = document.getElementById("btn-toggle-sched");
  btn.disabled = true;
  try {
    const res = await fetch(API + "/api/v1/ingest/scheduled/toggle", { method: "POST" });
    const data = await res.json();
    if (res.ok) {
      showSchedMsg(`DAG agora está ${data.label}.`, "success");
      loadScheduledStatus();
    } else {
      showSchedMsg(`Erro: ${JSON.stringify(data.detail || data)}`, "error");
    }
  } catch (e) {
    showSchedMsg(`Erro: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
  }
});

function showSchedMsg(text, type) {
  const el = document.getElementById("sched-msg");
  el.innerHTML = `<div class="ingest-msg ${type}"><pre>${escapeHtml(text)}</pre></div>`;
  setTimeout(() => { el.innerHTML = ""; }, 5000);
}

/* ===== INIT ===== */
loadPipeline();
loadSubreddits().catch(() => {});
checkAirflow().catch(() => {});
loadScheduledStatus().catch(() => {});
