const userId = "42";

const elements = {
  servedFrom: document.querySelector("#servedFrom"),
  trustStatus: document.querySelector("#trustStatus"),
  versionStatus: document.querySelector("#versionStatus"),
  dirtyStatus: document.querySelector("#dirtyStatus"),
  profileBadge: document.querySelector("#profileBadge"),
  redisBadge: document.querySelector("#redisBadge"),
  userId: document.querySelector("#userId"),
  userName: document.querySelector("#userName"),
  userPhone: document.querySelector("#userPhone"),
  userEmail: document.querySelector("#userEmail"),
  postgresState: document.querySelector("#postgresState"),
  redisState: document.querySelector("#redisState"),
  eventLog: document.querySelector("#eventLog"),
  updateForm: document.querySelector("#updateForm"),
  nameInput: document.querySelector("#nameInput"),
  phoneInput: document.querySelector("#phoneInput"),
  refreshBtn: document.querySelector("#refreshBtn"),
  clearLogBtn: document.querySelector("#clearLogBtn"),
};

const scenarioEndpoints = {
  reset: { method: "POST", path: "/demo/reset", title: "Reset" },
  rejected: {
    method: "POST",
    path: "/demo/rejected-write",
    title: "Rejected write",
    body: () => demoProfileBody(),
  },
  crash: {
    method: "POST",
    path: "/demo/crash-after-db-commit",
    title: "Crash after DB commit",
    body: () => demoProfileBody(),
  },
  repair: { method: "POST", path: "/repair/run-once", title: "Repair once" },
  race: {
    method: "POST",
    path: "/demo/delayed-after-race",
    title: "Delayed AFTER race",
    body: () => demoProfileBody(),
  },
};

let busy = false;

function demoProfileBody() {
  const name = elements.nameInput.value.trim();
  const phoneNumber = elements.phoneInput.value.trim();
  return {
    name: name || elements.userName.textContent,
    phone_number: phoneNumber || elements.userPhone.textContent,
  };
}

function pretty(value) {
  return JSON.stringify(value ?? {}, null, 2);
}

function setBadge(element, text, className) {
  element.textContent = text;
  element.className = `badge ${className}`;
}

function getDebug(payload) {
  if (payload?.debug) {
    return payload.debug;
  }

  return {
    postgres: payload?.postgres ?? null,
    redis: payload?.redis ?? {},
    dirty_keys: payload?.dirty_keys ?? [],
  };
}

function summarizePayload(payload) {
  if (payload?.served_from) {
    return `served_from=${payload.served_from}, trusted_cache=${payload.trusted_cache}`;
  }

  if (payload?.outcome) {
    if (payload.outcome === "simulated_crash_after_postgres_commit") {
      return `Postgres committed version ${payload.user?.version}; Redis AFTER was intentionally skipped`;
    }

    return payload.outcome;
  }

  if (Array.isArray(payload?.results)) {
    return payload.results.length
      ? payload.results.map((result) => `${result.key}: ${result.action}`).join(", ")
      : "no dirty keys";
  }

  return "completed";
}

function addEvent(title, payload, failed = false) {
  const item = document.createElement("li");
  const titleNode = document.createElement("span");
  const detailNode = document.createElement("span");

  titleNode.className = "event-title";
  detailNode.className = "event-detail";
  titleNode.textContent = `${new Date().toLocaleTimeString()} · ${title}`;
  detailNode.textContent = failed ? String(payload) : summarizePayload(payload);

  item.append(titleNode, detailNode);
  elements.eventLog.prepend(item);
}

function render(payload) {
  const debug = getDebug(payload);
  const redis = debug.redis ?? {};
  const user = payload?.user ?? redis.value ?? debug.postgres ?? {};
  const dirtyKeys = debug.dirty_keys ?? [];
  const servedFrom = payload?.served_from ?? "n/a";
  const trusted = payload?.trusted_cache ?? redis.trusted ?? false;

  elements.servedFrom.textContent = servedFrom;
  elements.trustStatus.textContent = trusted ? "trusted" : "untrusted";
  elements.versionStatus.textContent = user?.version ?? redis.version ?? "unknown";
  elements.dirtyStatus.textContent = dirtyKeys.length ? dirtyKeys.join(", ") : "none";

  elements.userId.textContent = user?.id ?? "unknown";
  elements.userName.textContent = user?.name ?? "unknown";
  elements.userPhone.textContent = user?.phone_number ?? "unknown";
  elements.userEmail.textContent = user?.email ?? "unknown";

  elements.postgresState.textContent = pretty(debug.postgres);
  elements.redisState.textContent = pretty(redis);

  elements.nameInput.value = user?.name ?? "";
  elements.phoneInput.value = user?.phone_number ?? "";

  setBadge(
    elements.profileBadge,
    servedFrom === "redis" ? "redis hit" : servedFrom === "postgres" ? "db fallback" : "scenario",
    servedFrom === "redis" ? "redis-source" : servedFrom === "postgres" ? "postgres-source" : "neutral",
  );

  setBadge(elements.redisBadge, trusted ? "trusted" : "untrusted", trusted ? "trusted" : "untrusted");
}

function setBusy(nextBusy) {
  busy = nextBusy;
  document.querySelectorAll("button, input").forEach((node) => {
    node.disabled = busy;
  });
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "content-type": "application/json", ...(options.headers ?? {}) },
    ...options,
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail ?? response.statusText);
  }

  return payload;
}

async function runAction(title, operation, refreshAfter = true) {
  if (busy) {
    return;
  }

  setBusy(true);
  try {
    const payload = await operation();
    render(payload);
    addEvent(title, payload);

    if (refreshAfter && !payload.served_from) {
      const latest = await request(`/users/${userId}`);
      render(latest);
    }
  } catch (error) {
    addEvent(title, error.message, true);
  } finally {
    setBusy(false);
  }
}

async function refresh() {
  await runAction("Refresh", () => request(`/users/${userId}`), false);
}

elements.updateForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await runAction("Normal update", () =>
    request(`/users/${userId}`, {
      method: "PATCH",
      body: JSON.stringify({
        name: elements.nameInput.value,
        phone_number: elements.phoneInput.value,
      }),
    }),
  );
});

elements.refreshBtn.addEventListener("click", refresh);
elements.clearLogBtn.addEventListener("click", () => {
  elements.eventLog.innerHTML = "";
});

document.querySelectorAll("[data-action]").forEach((button) => {
  button.addEventListener("click", async () => {
    const action = scenarioEndpoints[button.dataset.action];
    await runAction(action.title, () =>
      request(action.path, {
        method: action.method,
        body: action.body ? JSON.stringify(action.body()) : undefined,
      }),
    );
  });
});

refresh();
