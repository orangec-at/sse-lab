/**
 * What reconnecting costs you, with and without event ids.
 *
 *   node reconnect.mjs
 */

const PORT = Number(process.env.PORT ?? 8791)

/** Reads a route to completion or until the socket dies. Returns the text. */
async function read(route, headers = {}) {
  let text = ''
  let died = false
  try {
    const res = await fetch(`http://localhost:${PORT}/${route}`, { headers })
    const reader = res.body.getReader()
    const dec = new TextDecoder()
    let carry = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      carry += dec.decode(value, { stream: true })
      const parts = carry.split('\n\n')
      carry = parts.pop() ?? ''
      for (const p of parts) {
        const line = p.split('\n').find((l) => l.startsWith('data: '))
        if (!line) continue
        const payload = line.slice(6)
        if (payload === '[DONE]') return { text, died, complete: true }
        text += payload
      }
    }
  } catch {
    died = true
  }
  return { text, died, complete: false }
}

/** Pulls the last id out of a stream so we can resume from it. */
async function readTrackingId(route, headers = {}) {
  let text = ''
  let lastId = null
  try {
    const res = await fetch(`http://localhost:${PORT}/${route}`, { headers })
    const reader = res.body.getReader()
    const dec = new TextDecoder()
    let carry = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      carry += dec.decode(value, { stream: true })
      const parts = carry.split('\n\n')
      carry = parts.pop() ?? ''
      for (const p of parts) {
        const lines = p.split('\n')
        const id = lines.find((l) => l.startsWith('id: '))
        const data = lines.find((l) => l.startsWith('data: '))
        if (id) lastId = id.slice(4)
        if (!data) continue
        if (data.slice(6) === '[DONE]') return { text, lastId, complete: true }
        text += data.slice(6)
      }
    }
  } catch {
    /* socket died */
  }
  return { text, lastId, complete: false }
}

const show = (s) => (s.length > 66 ? s.slice(0, 66) + '…' : s)

console.log('\n  ── no ids: the shape almost every LLM API has ────────────────\n')

const first = await read('duplicating')
console.log(`  attempt 1  ${first.died ? 'socket died' : 'ended'}, complete=${first.complete}`)
console.log(`             painted: ${JSON.stringify(show(first.text))}`)

const retry = await read('duplicating', { 'x-retry': '1' })
console.log(`  attempt 2  reconnected, complete=${retry.complete}`)
console.log(`             received: ${JSON.stringify(show(retry.text))}`)

console.log(`\n  naive concatenation of what the user now sees:`)
console.log(`             ${JSON.stringify(show(first.text + retry.text))}`)
const overlap = first.text.length
console.log(`             the first ${overlap} characters are on screen twice.`)

console.log('\n  ── with ids: resumable, if the server cooperates ─────────────\n')

const a = await readTrackingId('resumable')
console.log(`  attempt 1  complete=${a.complete}, last id seen = ${a.lastId}`)
console.log(`             painted: ${JSON.stringify(show(a.text))}`)

const b = await readTrackingId('resumable', { 'Last-Event-ID': String(a.lastId) })
console.log(`  attempt 2  sent Last-Event-ID: ${a.lastId}, complete=${b.complete}`)
console.log(`             received: ${JSON.stringify(show(b.text))}`)
console.log(`\n  joined:    ${JSON.stringify(show(a.text + b.text))}`)
console.log('             no overlap: the server resumed where the client stopped.')

console.log(`
  The difference is not the client. Both clients reconnect. The difference is
  whether the payload carries identity the server can resume from.

  LLM delta streams do not. There is no id on a text_delta, and the model is
  not going to regenerate the identical tokens anyway. So "reconnect and
  continue" is not available — the honest options are:

    · retry only before anything is on screen        (cheap, correct, limited)
    · buffer server-side under a request id           (works, needs infra)
    · give up and show the partial answer with an error

  Retrying *after* first paint and appending is the one thing that is always
  wrong, and it is the default if you do not think about it.
`)
