export async function apiFetch(url, options = {}) {
  const resp = await fetch(url, options)
  const text = await resp.text()

  let data = {}
  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = { detail: text }
    }
  }

  if (!resp.ok) {
    const detail = data.detail || data.message || `HTTP ${resp.status}`
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail))
  }

  return data
}
