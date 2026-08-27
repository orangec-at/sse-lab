/**
 * Shows what a client actually receives, read by read.
 *
 *   node read.mjs chopped        what each read() returns
 *   node read.mjs chopped naive  what a per-chunk parser makes of it
 *   node read.mjs anthropic      typed blocks assembled from the events
 */

const PORT = Number(process.env.PORT ?? 8791)
const route = process.argv[2] ?? 'chopped'
const mode = process.argv[3] ?? 'reads'

const show = (s) => JSON.stringify(s)

const res = await fetch(`http://localhost:${PORT}/${route}`, {
  headers: { Accept: 'text/event-stream' },
})
const reader = res.body.getReader()
const decoder = new TextDecoder()

if (mode === 'reads') {
  // What the socket hands you. Not events — bytes, cut wherever TCP felt like.
  console.log('each line is one read() from the socket:\n')
  let n = 0
  let carry = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    const chunk = decoder.decode(value, { stream: true })
    carry += chunk
    console.log(`  read ${String(++n).padStart(3)}  ${show(chunk)}`)
  }
  console.log(`\n  ${n} reads`)
  console.log('  note how few of them line up with an event boundary.')
}

if (mode === 'naive') {
  // The bug everyone writes once: treat each chunk as if it were an event.
  console.log('a parser that assumes one chunk == one event:\n')
  let recovered = ''
  let dropped = 0
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    const chunk = decoder.decode(value, { stream: true })
    const m = /^data: (.*)$/m.exec(chunk)
    if (m) recovered += m[1]
    else dropped++
  }
  console.log(`  recovered: ${show(recovered)}`)
  console.log(`  chunks that matched nothing and were thrown away: ${dropped}`)
  console.log('\n  the text is mangled: every event split across a boundary is lost.')
}

if (mode === 'blocks') {
  // Assemble Anthropic's typed blocks. Note that no markdown parsing is needed
  // to know where a block ends - content_block_stop says so.
  const blocks = new Map()
  let carry = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    carry += decoder.decode(value, { stream: true })
    const parts = carry.split('\n\n')
    carry = parts.pop() ?? ''
    for (const part of parts) {
      const line = part.split('\n').find((l) => l.startsWith('data: '))
      if (!line) continue
      const e = JSON.parse(line.slice(6))
      if (e.type === 'content_block_start') {
        blocks.set(e.index, { type: e.content_block.type, text: '', done: false, name: e.content_block.name })
      } else if (e.type === 'content_block_delta') {
        const b = blocks.get(e.index)
        const d = e.delta
        b.text += d.text ?? d.thinking ?? d.partial_json ?? ''
      } else if (e.type === 'content_block_stop') {
        blocks.get(e.index).done = true
      }
    }
  }
  console.log('blocks assembled from the stream:\n')
  for (const [i, b] of blocks) {
    const label = b.name ? `${b.type} (${b.name})` : b.type
    console.log(`  [${i}] ${label.padEnd(22)} done=${b.done}`)
    console.log(`      ${show(b.text.length > 78 ? b.text.slice(0, 78) + '…' : b.text)}`)
  }
  const tool = [...blocks.values()].find((b) => b.type === 'tool_use')
  if (tool) {
    console.log(`\n  the tool_use block arrived as JSON fragments; parsed once at the end:`)
    console.log(`      ${show(JSON.parse(tool.text))}`)
  }
}
