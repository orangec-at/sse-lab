import { createServer } from 'node:http'
import { gzipSync } from 'node:zlib'

/**
 * A deliberately awkward SSE server.
 *
 * Every route reproduces one thing that actually goes wrong when a browser
 * consumes a token stream. Hit them with curl to see the bytes, or with the
 * pages in client.html to see what a parser has to survive.
 *
 *   node server.mjs          then  curl -N http://localhost:8791/<route>
 */

const PORT = Number(process.env.PORT ?? 8791)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const SSE_HEADERS = {
  'Content-Type': 'text/event-stream; charset=utf-8',
  'Cache-Control': 'no-cache, no-transform',
  Connection: 'keep-alive',
  // Tells nginx not to sit on the response. Without it a default nginx buffers
  // the whole stream and the client sees nothing until the request ends.
  'X-Accel-Buffering': 'no',
  'Access-Control-Allow-Origin': '*',
}

const ANSWER =
  'Server-Sent Events is a one-way channel: the server writes, the browser reads. ' +
  'It rides on a normal HTTP response that never ends, which is why it survives ' +
  'proxies and needs no special handshake.'

/** Splits a string into token-ish pieces, the way a model emits them. */
const tokenize = (s) => s.match(/\s*\S+/g) ?? []

const routes = {
  /**
   * 1. The format, one event at a time, cleanly framed.
   * This is what every tutorial shows and what almost nothing sends.
   */
  async plain(res) {
    res.writeHead(200, SSE_HEADERS)
    for (const token of tokenize(ANSWER)) {
      res.write(`data: ${token}\n\n`)
      await sleep(40)
    }
    res.write('data: [DONE]\n\n')
    res.end()
  },

  /**
   * 2. The same events, but written in chunks that cut wherever they like.
   *
   * TCP has no idea what an SSE event is. A write can land mid-field-name, and
   * two events can arrive glued together in one read. A parser that treats each
   * chunk as a message loses data here — this is the single most common SSE bug.
   */
  async chopped(res) {
    res.writeHead(200, SSE_HEADERS)
    const payload = tokenize(ANSWER).map((t) => `data: ${t}\n\n`).join('') + 'data: [DONE]\n\n'
    // Cut at 7 bytes regardless of where that lands.
    for (let i = 0; i < payload.length; i += 7) {
      res.write(payload.slice(i, i + 7))
      await sleep(15)
    }
    res.end()
  },

  /**
   * 3. Everything the wire format allows, most of which parsers forget.
   */
  async quirks(res) {
    res.writeHead(200, SSE_HEADERS)
    // A comment. Used as a heartbeat so idle proxies don't cut the connection.
    res.write(': keep-alive\n\n')
    await sleep(100)
    // Named event. The default when omitted is "message".
    res.write('event: greeting\ndata: hello\n\n')
    await sleep(100)
    // Multiple data lines join with \n. This is how you send a JSON blob that
    // itself contains newlines without escaping them.
    res.write('data: line one\ndata: line two\ndata: line three\n\n')
    await sleep(100)
    // id: sets Last-Event-ID for reconnects. retry: sets the reconnect delay.
    res.write('id: 42\nretry: 3000\ndata: with an id\n\n')
    await sleep(100)
    // A field with no space after the colon. The space is framing, not data:
    // "data:tight" and "data: tight" both mean "tight".
    res.write('data:tight\n\n')
    await sleep(100)
    // Unknown fields are ignored, not errors.
    res.write('nonsense: ignore me\ndata: survived\n\n')
    await sleep(100)
    // CRLF line endings are legal.
    res.write('event: crlf\r\ndata: windows\r\n\r\n')
    await sleep(100)
    // A final event with no trailing blank line, then the socket closes.
    // A parser that only emits on a blank line silently drops this one.
    res.write('data: unterminated tail')
    res.end()
  },

  /**
   * 4. Anthropic's shape: a message is a sequence of typed, indexed blocks.
   *
   * Note what the protocol hands you for free: content_block_stop says "this
   * block will never change again". A UI does not have to guess where a
   * paragraph ended — and thinking, text and tool_use are separated at the
   * source rather than concatenated into one string.
   */
  async anthropic(res) {
    res.writeHead(200, SSE_HEADERS)
    const send = (event, data) =>
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)

    send('message_start', {
      type: 'message_start',
      message: { id: 'msg_demo', type: 'message', role: 'assistant', content: [] },
    })

    // Block 0: thinking. A chat UI usually renders this collapsed.
    send('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'thinking', thinking: '' } })
    for (const t of tokenize('The user asked what SSE is. Keep it short.')) {
      send('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'thinking_delta', thinking: t } })
      await sleep(30)
    }
    send('content_block_stop', { type: 'content_block_stop', index: 0 })

    // Block 1: visible text.
    send('content_block_start', { type: 'content_block_start', index: 1, content_block: { type: 'text', text: '' } })
    for (const t of tokenize('SSE is a one-way stream of text events over plain HTTP.')) {
      send('content_block_delta', { type: 'content_block_delta', index: 1, delta: { type: 'text_delta', text: t } })
      await sleep(30)
    }
    send('content_block_stop', { type: 'content_block_stop', index: 1 })

    // Block 2: a tool call. The arguments arrive as JSON *fragments* that are
    // not individually parseable — you must concatenate, then parse once.
    send('content_block_start', {
      type: 'content_block_start',
      index: 2,
      content_block: { type: 'tool_use', id: 'toolu_demo', name: 'get_weather', input: {} },
    })
    for (const frag of ['{"loc', 'ation":', ' "Seo', 'ul"}']) {
      send('content_block_delta', { type: 'content_block_delta', index: 2, delta: { type: 'input_json_delta', partial_json: frag } })
      await sleep(60)
    }
    send('content_block_stop', { type: 'content_block_stop', index: 2 })

    send('message_delta', { type: 'message_delta', delta: { stop_reason: 'tool_use' }, usage: { output_tokens: 41 } })
    send('message_stop', { type: 'message_stop' })
    res.end()
  },

  /**
   * 5. OpenAI's shape: one undifferentiated text stream, terminated by a
   * sentinel string rather than an event.
   *
   * There are no block boundaries here at all. If a UI wants to freeze finished
   * paragraphs it has to infer the boundaries from the text itself.
   */
  async openai(res) {
    res.writeHead(200, SSE_HEADERS)
    for (const t of tokenize('SSE is a one-way stream over plain HTTP.')) {
      res.write(`data: ${JSON.stringify({ choices: [{ delta: { content: t } }] })}\n\n`)
      await sleep(40)
    }
    res.write('data: [DONE]\n\n')
    res.end()
  },

  /**
   * 6. The stream that looks broken but is not: gzip without flushing.
   *
   * The bytes all arrive — at the end, in one lump. Compression middleware that
   * buffers until close turns a stream into a slow non-stream, and it is
   * invisible in the response body. This is why SSE responses set
   * `Cache-Control: no-transform` and why you disable compression for them.
   */
  async buffered(res) {
    const body = tokenize(ANSWER).map((t) => `data: ${t}\n\n`).join('') + 'data: [DONE]\n\n'
    res.writeHead(200, { ...SSE_HEADERS, 'Content-Encoding': 'gzip' })
    await sleep(1500) // pretend the buffer fills slowly
    res.end(gzipSync(Buffer.from(body)))
  },

  /**
   * 7. A stream that dies mid-message.
   *
   * Note what the client gets: a clean end-of-stream, not an error. There is no
   * "the response was incomplete" signal in the protocol — if you need one, the
   * payload has to carry it (which is what [DONE] and message_stop are for).
   */
  async truncated(res) {
    res.writeHead(200, SSE_HEADERS)
    const tokens = tokenize(ANSWER)
    for (const t of tokens.slice(0, 6)) {
      res.write(`data: ${t}\n\n`)
      await sleep(60)
    }
    res.destroy() // socket gone, no terminator
  },

  /**
   * 9. Firehose: writes as fast as the socket will take it, and reports when
   * the socket pushes back.
   *
   * `res.write()` returns false once Node's outgoing buffer is past its high
   * water mark — which happens when the kernel's send buffer is full, which
   * happens when the peer stops reading. That is TCP backpressure arriving in
   * userland. A server that ignores the return value and keeps writing grows
   * its own heap instead of slowing down.
   */
  async firehose(req, res) {
    res.writeHead(200, SSE_HEADERS)
    const started = Date.now()
    let written = 0
    let blockedAt = null
    let drains = 0

    while (Date.now() - started < 3000) {
      const ok = res.write(`data: ${'x'.repeat(512)}\n\n`)
      written++
      if (!ok) {
        if (blockedAt === null) blockedAt = written
        drains++
        // Wait for the socket to report it has room again.
        await new Promise((r) => res.once('drain', r))
      }
    }
    process.stdout.write(
      `  [firehose] wrote ${written} events, first blocked at #${blockedAt ?? '-'}, waited for drain ${drains}x\n`
    )
    res.end()
  },

  /**
   * 10. Reconnect that duplicates, because the payload has no ids.
   *
   * This is the shape almost every LLM API has: deltas with no identity. A
   * client that reconnects has no way to say where it left off, so it gets the
   * whole answer again — and whatever it already painted is now doubled.
   */
  async duplicating(req, res) {
    res.writeHead(200, SSE_HEADERS)
    const tokens = tokenize(ANSWER)
    // No id: fields anywhere. Nothing to resume from.
    for (let i = 0; i < tokens.length; i++) {
      res.write(`data: ${tokens[i]}\n\n`)
      await sleep(40)
      if (i === 5 && !req.headers['x-retry']) return res.destroy()
    }
    res.write('data: [DONE]\n\n')
    res.end()
  },

  /**
   * 8. Reconnect handling. EventSource retries automatically and sends the last
   * id it saw in a `Last-Event-ID` header; this route resumes from it.
   */
  async resumable(req, res) {
    const last = Number(req.headers['last-event-id'] ?? 0)
    res.writeHead(200, SSE_HEADERS)
    const tokens = tokenize(ANSWER)
    if (last > 0) res.write(`: resuming after id ${last}\n\n`)
    for (let i = last; i < tokens.length; i++) {
      res.write(`id: ${i + 1}\ndata: ${tokens[i]}\n\n`)
      await sleep(80)
      // Drop the connection a third of the way in, once.
      if (i === Math.floor(tokens.length / 3) && last === 0) return res.destroy()
    }
    res.write('data: [DONE]\n\n')
    res.end()
  },
}

const INDEX = `SSE lab — http://localhost:${PORT}

  /plain       clean framing, one event per token
  /chopped     same events, cut at arbitrary byte offsets
  /quirks      comments, multi-line data, ids, CRLF, unterminated tail
  /anthropic   typed indexed blocks (thinking / text / tool_use)
  /openai      one flat text stream + [DONE] sentinel
  /buffered    gzip without flush — arrives all at once at the end
  /truncated   socket dies mid-stream, no terminator
  /resumable   drops once, resumes via Last-Event-ID
  /firehose    writes flat out; reports when the socket pushes back
  /duplicating drops once, no ids — reconnect replays from the top

Try:  curl -N http://localhost:${PORT}/chopped
      curl -N --raw -D - http://localhost:${PORT}/plain
Open: client.html (any static server, or just open the file)
`

createServer(async (req, res) => {
  const path = new URL(req.url, `http://localhost:${PORT}`).pathname.slice(1)
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*' })
    return res.end()
  }
  const route = routes[path]
  if (!route) {
    res.writeHead(path === '' ? 200 : 404, { 'Content-Type': 'text/plain; charset=utf-8' })
    return res.end(INDEX)
  }
  try {
    await (route.length > 1 ? route(req, res) : route(res))
  } catch {
    // Client hung up mid-write; nothing to do.
  }
}).listen(PORT, () => console.log(INDEX))
