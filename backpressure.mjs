/**
 * Does a slow reader actually slow the server down?
 *
 *   node backpressure.mjs
 *
 * Three consumers against the same firehose route. Watch the server's own log
 * line (it prints how many events it managed to write and how often the socket
 * told it to wait) next to what each consumer received.
 */

const PORT = Number(process.env.PORT ?? 8791)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function consume(label, perReadDelayMs) {
  const t0 = Date.now()
  const res = await fetch(`http://localhost:${PORT}/firehose`)
  const reader = res.body.getReader()
  let reads = 0
  let bytes = 0

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    reads++
    bytes += value.byteLength
    // A render that costs real time. This is the whole point: the consumer is
    // the slow part, not the network.
    if (perReadDelayMs > 0) await sleep(perReadDelayMs)
  }

  const ms = Date.now() - t0
  console.log(
    `  ${label.padEnd(26)} reads ${String(reads).padStart(5)}` +
      ` | ${(bytes / 1024 / 1024).toFixed(2).padStart(6)} MB` +
      ` | ${String(ms).padStart(5)}ms`
  )
}

console.log('\n  server writes flat out for 3s; the consumer varies\n')

console.log('  ── fast consumer: read and discard ───────────────────────────')
await consume('no delay', 0)
await sleep(400)

console.log('\n  ── slow consumer: 5ms of work per read ───────────────────────')
await consume('5ms per read', 5)
await sleep(400)

console.log('\n  ── very slow consumer: 40ms per read ─────────────────────────')
await consume('40ms per read', 40)

console.log(`
  Read the server's [firehose] lines above.

  The slow consumers do not make the server produce less data into a growing
  buffer — they make it *stop writing*. res.write() returns false, the server
  awaits 'drain', and the kernel's send window does the rest. Backpressure over
  a fetch stream is automatic and it reaches all the way back to the producer.

  That is only true while the bytes are still in the socket. Once you have read
  a chunk, it is in your heap and no one is pushing back any more — which is
  where a chat UI actually gets into trouble: not the network, the render.
`)
