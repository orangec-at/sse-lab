# 🧪 SSE Lab: LLM 스트리밍 엔지니어링 & 실험실

> **Server-Sent Events(SSE)**가 실제 네트워크(TCP)와 LLM 토큰 스트림 환경에서 어떻게 동작하고, 어디서 데이터가 깨지는지 직접 확인하고 학습하는 종합 실험실입니다.  
> **Rust 비동기 백엔드** + **Next.js 대시보드** + **Web Worker 기반 Rust WebAssembly 멀티스레드 파서 & 벤치마크 엔진**으로 구성되어 있습니다.

---

## 🏛 시스템 아키텍처

```
┌────────────────────────────────────────────────────────────────────────┐
│ [외부 독립 서버] server-rs (Rust Axum + Tokio)                         │
│ 포트: 8791 (text/event-stream)                                         │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ (TCP Socket Chunk Stream)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ [프론트엔드 Consumer] web/ (Next.js 15 App Router + TypeScript)        │
│                                                                        │
│  [3가지 파서 아키텍처 지원]                                            │
│  ├── 1) Pure TypeScript Parser   : V8 JIT 기반 Carry-Buffer 파서       │
│  ├── 2) Rust WebAssembly (Main)  : Wasm 선형 메모리 기반 파서          │
│  └── 3) 🚀 Worker + Wasm (Thread): 별도 OS 백그라운드 스레드 파싱       │
│                                    (Zero UI Freeze & 60fps 보장)       │
│                                                                        │
│  ├── ⚡ Live Benchmark Modal    : FFI 오버헤드 vs Micro-Batching 분석 │
│  └── app/api/proxy/              : BFF Streaming Relay (Proxy)         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 1. Web Worker + Rust Wasm 아키텍처 (Zero UI Freeze)

대규모 LLM 토큰 스트림(예: 초당 1,000+ 토큰 방출) 수신 시, 메인 스레드에서 파싱과 렌더링을 동시에 처리하면 **프레임 드롭(Jank)과 UI 멈춤 현상**이 발생합니다.

이를 완벽하게 해결하기 위해 **Web Worker 전용 백그라운드 스레드**를 도입했습니다:
* **완전한 스레드 분리**: `fetch()` 스트림 소비와 Wasm Carry-Buffer 파싱을 백그라운드 OS 스레드(`sse-stream.worker.js`)에서 전담.
* **마이크로 배칭(Micro-Batching)**: 1KB / 16ms 렌더 프레임 단위로 Wasm 파싱을 일괄 수행하여 FFI 오버헤드를 제거하고 메인 스레드로 배치 전달.
* **60fps UI 보장**: 대시보드 상단의 **`UI Main Thread: 60 FPS`** 모니터를 통해 고속 스트리밍 중에도 UI가 매끄럽게 유지됨을 실시간 확인.

---

## ⚡ 2. Wasm FFI 바운더리 비용과 Micro-Batching

```
[❌ Naive 안티패턴]
소켓 청크(64B) 수신 ──► JS String ──► Wasm 메모리 복사 ──► Rust 파싱 ──► Serde 역직렬화 (1초에 수천 번 반복 = FFI 병목)

[✅ 실무 권장 패턴: Micro-Batching]
소켓 청크들을 1KB/16ms 버퍼에 모음 ──► Wasm 1회 일괄 전달 ──► FFI 호출 1/100 급감 (V8 JIT 대비 2배+ 빠름)
```

상단의 **`⚡ Live Benchmark`**를 실행하면 아래 4단계 아키텍처의 실제 처리량 차이를 확인할 수 있습니다:
1. **Pure JavaScript (V8 JIT)**: ~2.5M ev/s (Baseline)
2. **Rust Wasm (Naive Per-Chunk)**: ~1.4M ev/s (작은 청크마다 FFI 호출로 인한 손해)
3. **Rust Wasm (Micro-Batched ⭐)**: ~4.5M ev/s (실무 권장 최적 패턴)
4. **Rust Wasm (Native Linear Memory)**: ~8.0M ev/s (순수 Rust 네이티브 한계 속도)

---

## 📚 3. SSE(Server-Sent Events)의 본질

### "SSE는 프로토콜이 아니라, 끝나지 않는 HTTP 응답 규격이다"

WebSocket은 별도의 핸드셰이크를 통해 TCP 프로토콜 자체를 전환(`ws://`)하지만, SSE는 **표준 HTTP/1.1 or HTTP/2 응답**을 그대로 사용합니다:

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-cache, no-transform
Connection: keep-alive
X-Accel-Buffering: no
Transfer-Encoding: chunked
```

* **Content-Length가 없음**: 응답 길이가 정해져 있지 않고 연결이 유지되며 `chunked`로 데이터가 지속적으로 밀려옵니다.
* **단방향(Server -> Client)**: 서버에서 클라이언트로 토큰이 내려오기만 하면 되는 LLM 채팅/스트리밍에 가장 가볍고 적합합니다.
* **방화벽/프록시 친화적**: 기존 HTTP 포트(80/443)를 그대로 통과하므로 사내 방화벽이나 CDN에서도 문제없이 동작합니다.

---

## ⚠️ 4. 핵심 원리: "청크(Chunk)는 이벤트(Event)가 아니다"

가장 흔하게 발생하는 스트리밍 파서 버그는 **"소켓에서 1번 읽어 들인 청크(read chunk)가 1개의 완성된 이벤트일 것"**이라고 잘못 가정하는 것입니다.

```
[TCP 소켓 수신 예시]
  read 1:  "data: S"
  read 2:  "erver-S"
  read 3:  "ent\n\nda"      ← 앞선 이벤트 끝(\n\n) + 다음 이벤트 시작이 1개 청크에 뭉침
  read 4:  "ta:  Ev"
```

TCP는 애플리케이션의 `\n\n` 경계를 전혀 알지 못합니다. 필드명 중간(`"da"`, `"ta: "`)에서 잘리거나, 여러 이벤트가 한 번에 합쳐져서 도착합니다.

### 💡 해결책: Carry Buffer 알고리즘

```typescript
// 남은 미완성 조각(carry)을 들고 있다가 다음 청크와 이어 붙입니다.
carry += chunk;
const parts = carry.split('\n\n');
carry = parts.pop() ?? ''; // 마지막은 아직 덜 끝난 조각이므로 보관

for (const part of parts) {
  handleEvent(part); // 완성된 이벤트만 안전하게 파싱
}
```

---

## 🔬 5. 10가지 시나리오별 상세 분석 (Lab Scenarios)

대시보드(`http://localhost:3000`)에서 아래 시나리오들을 직접 실행하며 결과를 비교할 수 있습니다:

| 시나리오 라우트 | 현상 및 재현 내용 | 기술적 원인 및 프로덕션 교훈 |
|---|---|---|
| **`/plain`** | 교과서적인 1:1 토큰 스트림 | 튜토리얼에서만 볼 수 있는 이상적인 형태. 실제 네트워크 환경에서는 절대 보장되지 않음. |
| **`/chopped`** | 7바이트 단위로 임의 절단 전송 | **가장 중요한 실습.** Carry 버퍼 없이 정규식으로 청크를 파싱하면 90% 이상의 토큰이 유실됨을 확인. |
| **`/quirks`** | W3C SSE 사양 엣지 케이스 종합 | • `: ping` (주석: 유휴 방화벽 타임아웃 방지 하트비트)<br>• `data:tight` vs `data: tight` (콜론 뒤 첫 공백은 프레이밍이지 데이터가 아님)<br>• `\r\n` (CRLF 개행)<br>• `Unterminated tail` (빈 줄 없이 닫힐 때 `flush()` 처리) |
| **`/anthropic`** | Claude 방식의 구조화된 블록 스트림 | `thinking`, `text`, `tool_use`가 명시적 인덱스와 `content_block_stop`으로 전달됨. `tool_use`는 분할된 JSON 조각을 모아서 1회 파싱해야 함. |
| **`/openai`** | OpenAI 방식의 단일 델타 스트림 | 구조적 블록 없이 단일 텍스트 델타가 오며, 스트림 끝은 `[DONE]` 센티넬 문자열로 구분. |
| **`/buffered`** | Gzip 압축 버퍼링 함정 | 미들웨어가 스트림을 즉시 플러시하지 않고 압축 버퍼에 모아두면, 1.5초 뒤 마지막에 한 번에 도착함(TTFB 급증). |
| **`/truncated`** | `[DONE]` 없이 소켓 강제 종료 | HTTP 수준에서는 에러가 아니라 정상 종료(`done: true`)로 보고되므로, 애플리케이션 계층에서 완료 여부를 검증해야 함. |
| **`/resumable`** | `Last-Event-ID` 기반 단절 복구 | 스트림이 중간에 끊겼을 때 클라이언트가 마지막 수신 ID를 헤더에 담아 재요청하면, 서버가 해당 지점부터 이어서 전송. |
| **`/duplicating`** | ID 없는 재연결 시 텍스트 중복 현상 | 실제 상용 LLM API는 토큰 ID가 없어서 단순 Retry 시 이미 출력된 텍스트가 중복으로 렌더링되는 문제 시연. |
| **`/firehose`** | 초고속 토큰 방출 & 백프레셔 | 고속 전송 시 브라우저 렌더링 스레드 지연과 OS 커널 소켓 버퍼의 백프레셔 동작 확인. |

---

## 🛠 6. 프로덕션 SSE 운영 시 필수 체크리스트

1. **Nginx / ALB 버퍼링 해제**
   * Nginx: `X-Accel-Buffering: no` 헤더 전송 또는 `proxy_buffering off;`
   * Cloudflare: `Cache-Control: no-transform` 필수
2. **UTF-8 멀티바이트 분할 디코딩**
   * 한글이나 이모지는 3~4바이트입니다. 바이트 단위로 쪼개져 들어올 때 글자가 깨지지 않도록 `new TextDecoder('utf-8', { stream: true })` 옵션을 반드시 사용해야 합니다.
3. **Keep-Alive Heartbeat 전송**
   * 장시간 유휴 연결이 프록시 타임아웃(보통 60초)으로 끊어지지 않도록 15~30초마다 `: ping\n\n` 주석을 전송합니다.
4. **HTTP/2 사용 권장**
   * HTTP/1.1에서는 브라우저당 도메인별 최대 연결 수가 6개로 제한되어 여러 탭에서 SSE를 열면 블로킹됩니다. HTTP/2 멀티플렉싱을 적용하면 단일 커넥션으로 수백 개의 스트림을 지원합니다.

---

## 🚀 7. 실행 방법

### 사전 요구사항
* Node.js >= 18
* Rust & Cargo (1.80+)
* wasm-pack

### 실행 명령어

```bash
# 1. 의존성 설치 및 Wasm 빌드
npm install
npm run wasm:build
cd web && npm install && cd ..

# 2. Rust SSE Mock 서버 실행 (포트 8791)
npm run server:rs

# 3. Next.js 클라이언트 대시보드 실행 (포트 3000)
npm run web
```

브라우저에서 **`http://localhost:3000`**으로 접속하여 대시보드 및 Wasm 벤치마크를 확인하세요.
