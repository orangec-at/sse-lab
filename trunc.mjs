const r = await fetch('http://localhost:8791/truncated')
const rd = r.body.getReader()
let txt = ''
try {
  while (true) {
    const { done, value } = await rd.read()
    if (done) { console.log('  reader: done=true   ← 에러가 아니라 정상 종료로 보입니다'); break }
    txt += new TextDecoder().decode(value)
  }
} catch (e) {
  console.log('  예외:', e.constructor.name, '|', e.message)
}
console.log('  받은 내용:', JSON.stringify(txt.replace(/\n\n/g, ' | ').trim()))
console.log('  [DONE] 없음 → 페이로드에 종료 신호가 없으면 잘렸는지 구분 불가')
