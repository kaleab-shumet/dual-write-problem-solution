const userId = "42";

const el = {
  servedFrom: document.querySelector("#servedFrom"),
  cacheStatus: document.querySelector("#cacheStatus"),
  version: document.querySelector("#version"),
  avatar: document.querySelector("#avatar"),
  name: document.querySelector("#name"),
  userId: document.querySelector("#userId"),
  phone: document.querySelector("#phone"),
  email: document.querySelector("#email"),
  nameInput: document.querySelector("#nameInput"),
  phoneInput: document.querySelector("#phoneInput"),
  updateForm: document.querySelector("#updateForm"),
  refreshBtn: document.querySelector("#refreshBtn"),
  message: document.querySelector("#message"),
};

const scenarioActions = {
  reset: {
    label: "Reset complete.",
    path: "/demo/reset",
    method: "POST",
  },
  rejected: {
    label: "Rejected write sent. Final read completed.",
    path: "/demo/rejected-write",
    method: "POST",
    body: () => inputBody(),
  },
  crash: {
    label: "Crash-after-commit sent. Final read completed.",
    path: "/demo/crash-after-db-commit",
    method: "POST",
    body: () => inputBody(),
  },
  repair: {
    label: "Repair completed. Final read completed.",
    path: "/repair/run-once",
    method: "POST",
  },
  race: {
    label: "Delayed race sent. Final read completed.",
    path: "/demo/delayed-after-race",
    method: "POST",
    body: () => inputBody(),
  },
};

function initials(name) {
  return String(name || "?")
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0].toUpperCase())
    .join("");
}

function setBusy(busy) {
  document.querySelectorAll("button, input").forEach((node) => {
    node.disabled = busy;
  });
}

function inputBody() {
  return {
    name: el.nameInput.value,
    phone_number: el.phoneInput.value,
  };
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "content-type": "application/json", ...(options.headers ?? {}) },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.detail || response.statusText);
  }
  return payload;
}

function render(payload) {
  const user = payload.user;
  const servedFrom = payload.served_from;
  const trusted = payload.trusted_cache;

  el.servedFrom.textContent = servedFrom;
  el.servedFrom.className = servedFrom === "redis" ? "from-redis" : "from-postgres";
  el.cacheStatus.textContent = trusted ? "trusted" : "fallback";
  el.cacheStatus.className = trusted ? "from-redis" : "untrusted";
  el.version.textContent = user.version;

  el.avatar.textContent = initials(user.name);
  el.name.textContent = user.name;
  el.userId.textContent = user.id;
  el.phone.textContent = user.phone_number;
  el.email.textContent = user.email;

  el.nameInput.value = user.name;
  el.phoneInput.value = user.phone_number;
}

async function refresh(message = "Profile refreshed.") {
  setBusy(true);
  try {
    const payload = await request(`/users/${userId}`);
    render(payload);
    el.message.textContent = message;
  } catch (error) {
    el.message.textContent = error.message;
  } finally {
    setBusy(false);
  }
}

el.updateForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  setBusy(true);
  try {
    await request(`/users/${userId}`, {
      method: "PATCH",
      body: JSON.stringify({
        name: el.nameInput.value,
        phone_number: el.phoneInput.value,
      }),
    });
    await refresh("Changes saved.");
  } catch (error) {
    el.message.textContent = error.message;
    setBusy(false);
  }
});

el.refreshBtn.addEventListener("click", () => refresh());

document.querySelectorAll("[data-action]").forEach((button) => {
  button.addEventListener("click", async () => {
    const action = scenarioActions[button.dataset.action];
    setBusy(true);
    try {
      await request(action.path, {
        method: action.method,
        body: action.body ? JSON.stringify(action.body()) : undefined,
      });
      await refresh(action.label);
    } catch (error) {
      el.message.textContent = error.message;
      setBusy(false);
    }
  });
});

refresh("");
