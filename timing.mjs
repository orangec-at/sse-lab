const PORT = Number(process.env.PORT ?? 8791)
for (const route of ['plain', 'buffered']) {
  const t0 = Date.now()
  const res = await fetch(`http://localhost:${PORT}/${route}`)
  const reader = res.body.getReader()
  let first = null, n = 0
  while (true) {
    const { done } = await reader.read()
    if (done) break
    if (first === null) first = Date.now() - t0
    n++
  }
  const total = Date.now() - t0
  console.log(`  ${route.padEnd(10)} 첫 바이트 ${String(first).padStart(5)}ms | 전체 ${String(total).padStart(5)}ms | 청크 ${String(n).padStart(3)}개  ${route === 'buffered' ? '← 스트리밍이 아님' : ''}`)
}
