// The server injects window.SQLNOW_SCOPE (the session id) into index.html,
// so browser-stored preferences are keyed per session. Without one (plain
// in-memory runs, vite dev server) keys are unscoped, as before.
export function storageKey(suffix) {
  const scope = window.SQLNOW_SCOPE;
  return scope ? `sqlnow-${scope}-${suffix}` : `sqlnow-${suffix}`;
}

async function apiJson(url, options = {}) {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (res.status === 204) {
    return null;
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `${res.status} ${res.statusText}`);
  }
  return data;
}

export function fetchQueries() {
  return apiJson('/api/queries');
}

export function fetchQuery(name) {
  return apiJson('/api/queries/' + encodeURIComponent(name));
}

export function createQuery(body = {}) {
  return apiJson('/api/queries', { method: 'POST', body: JSON.stringify(body) });
}

export function updateQuery(name, body) {
  return apiJson('/api/queries/' + encodeURIComponent(name), {
    method: 'PUT',
    body: JSON.stringify(body),
  });
}

export function deleteQueryApi(name) {
  return apiJson('/api/queries/' + encodeURIComponent(name), { method: 'DELETE' });
}

export function fetchHistory(limit = 200) {
  return apiJson('/api/history?limit=' + limit);
}

export function queryPath(name) {
  return '/queries/' + encodeURIComponent(name);
}
