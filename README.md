# 🧪 SSE Lab: LLM 스트리밍 엔지니어링 & 프로덕션 시스템 디자인

> **Server-Sent Events(SSE)**가 실제 네트워크(TCP)와 LLM 토큰 스트림 환경에서 어떻게 동작하고 어디서 데이터가 깨지는지 학습하는 실습장이자,  
> **Rust 비동기 백엔드 + Next.js BFF + Web Worker & Rust Wasm 멀티스레딩**으로 이어지는 **프로덕션 4계층 시스템 디자인(Production 4-Tier System Design)** 프로젝트입니다.

---

## 🏛 1. 프로덕션 시스템 디자인 아키텍처 (Production 4-Tier Architecture)

실제 대규모 AI 스트리밍 서비스를 운영하는 기업 관점에서 각 컴포넌트가 배치되는 인프라 계층(Layer)과 데이터 흐름입니다.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ [Layer 1] Edge & CDN 계층 (CloudFront / Cloudflare / Fastly)                │
│                                                                             │
│  • Next.js HTML/JS 정적 번들                                                │
│  • 🦀 wasm_parser_bg.wasm (80KB 바이너리)  ──► 브라우저가 다운로드 후       │
│  • 🚀 sse-stream.worker.js (워커 스크립트) ──► 사용자 PC CPU 코어에서 실행   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ (API 및 스트림 요청)
┌─────────────────────────────────────────────────────────────────────────────┐
│ [Layer 2] L7 Gateway / Ingress 계층 (AWS ALB / Envoy / Nginx Ingress)       │
│                                                                             │
│  • Path Routing:                                                            │
│     - `GET /v1/stream/*` ──► Layer 4 (Rust SSE 백엔드) 직결 (Buffering Off) │
│     - `/*`               ──► Layer 3 (Next.js Node 컨테이너)                │
│  • HTTP/2 종단(Termination) & 300초 장기 커넥션 타임아웃 유지               │
└───────────────────────┬─────────────────────────────┬───────────────────────┘
                        │                             │
                        ▼                             ▼
┌────────────────────────────────┐ ┌──────────────────────────────────────────┐
│ [Layer 3] Web / BFF 계층       │ │ [Layer 4] Real-time Streaming Backend 계층│
│ (K8s Node Pod / ECS Fargate)   │ │ (K8s Rust Pod / ECS DaemonSet)           │
│                                │ │                                          │
│ • Next.js App Router (SSR)     │ │ • Rust (Axum + Tokio) SSE Core           │
│ • 세션 인증, 권한 검증, 보안    │ │ • 수십만 개 장기 소켓 커넥션 무중단 유지 │
│ • 비-스트리밍 일반 REST API    │ │ • 초경량 메모리 (컨테이너 크기 < 15MB)   │
└────────────────────────────────┘ └──────────────────────────────────────────┘
```

### 🔍 계층별 기술적 의사결정 (Why Architecture Matters)

| 계층 | 기술 스택 | 배포 위치 | 핵심 역할 및 아키텍처적 이유 |
|---|---|---|---|
| **Layer 1<br>(Edge / Client)** | **Web Worker + Rust Wasm** | S3 / CloudFront (CDN) | **서버 CPU가 아닌 사용자 PC 코어로 파싱 부하 분산.** 메인 스레드와 완전 격리된 별도 백그라운드 스레드에서 Wasm 파싱을 수행하여 초고속 스트림 중에도 **60 FPS UI 무중단 렌더링** 보장. |
| **Layer 2<br>(L7 Ingress)** | **Nginx / Envoy / AWS ALB** | Ingress Gateway | 스트리밍 경로(`/v1/stream/*`)에 `proxy_buffering off;` 및 `timeout 300s`를 적용하여 중간 버퍼링으로 인한 토큰 지연 방지. 브라우저 커넥션 제한 해소를 위한 **HTTP/2 멀티플렉싱** 적용. |
| **Layer 3<br>(Web / BFF)** | **Next.js 15 (Node.js)** | K8s Pod / Vercel | SSR 화면 렌더링 및 클라이언트에 노출되면 안 되는 API Key/인증 토큰을 안전하게 숨겨주는 경량 프록시(BFF) 역할. |
| **Layer 4<br>(Stream Core)** | **Rust (Axum + Tokio)** | K8s Pod / ECS Fargate | Node.js 대비 수십 배 적은 메모리로 **수십만 개의 장기 HTTP 커넥션을 유지**하며, LLM 토큰을 논블로킹 epoll/kqueue 이벤트 루프로 클라이언트에 밀어주는 실시간 전용 백엔드. |

---

## ⚡ 2. 클라이언트 내부 시스템 디자인: Web Worker & Micro-Batching

```
[메인 스레드 (UI Thread)]
  • 60 FPS Heartbeat 모니터 (Jank 감지)
  • UI 렌더링에만 100% 집중
       ▲
       │ postMessage (16ms Micro-Batched Events 전달)
       │
[Web Worker (백그라운드 OS 스레드)]
  • sse-stream.worker.js
  • fetch() ReadableStream 수신
  • 🦀 Rust Wasm Carry-Buffer 파싱 (Zero GC, 선형 메모리 일괄 처리)
```

### 💡 FFI 바운더리 비용 극복: Micro-Batching
* **문제**: 작은 청크(64B)마다 JS ↔ Wasm 경계를 넘나들면(FFI 호출) 직렬화 오버헤드로 인해 V8 JIT보다 느려집니다.
* **해결책**: 16ms(60fps 렌더링 주기) 또는 1KB 단위로 버퍼를 모아 Wasm으로 1회 일괄 전달하는 **Micro-Batching**을 적용하여 FFI 비용을 95% 이상 제거.

대시보드 상단의 **`⚡ Live Benchmark`**를 실행하면 아래 4단계 아키텍처의 실제 처리량 차이를 직접 확인할 수 있습니다:
1. **Pure JavaScript (V8 JIT)**: ~2.5M ev/s (Baseline)
2. **Rust Wasm (Naive Per-Chunk)**: ~1.4M ev/s (작은 청크마다 FFI 호출로 인한 손해)
3. **Rust Wasm (Micro-Batched ⭐)**: ~4.5M ev/s (실무 권장 최적 패턴 - JS 대비 2x 빠름)
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

## 🔬 5. 10가지 실험실 시나리오 (Lab Scenarios)

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

## 🛠 6. 프로덕션 인프라 체크리스트

1. **Nginx / ALB 버퍼링 해제**
   * Nginx: `proxy_buffering off;` 및 `X-Accel-Buffering: no` 헤더 필수.
   * Cloudflare: `Cache-Control: no-transform` 필수.
2. **UTF-8 멀티바이트 분할 디코딩**
   * 한글이나 이모지는 3~4바이트입니다. 바이트 단위로 쪼개져 들어올 때 글자가 깨지지 않도록 `new TextDecoder('utf-8', { stream: true })` 옵션을 반드시 사용해야 합니다.
3. **Keep-Alive Heartbeat 전송**
   * 장시간 유휴 연결이 프록시 타임아웃(보통 60초)으로 끊어지지 않도록 15~30초마다 `: ping\n\n` 주석을 전송합니다.
4. **HTTP/2 사용 권장**
   * HTTP/1.1에서는 브라우저당 도메인별 최대 연결 수가 6개로 제한되어 여러 탭에서 SSE를 열면 블로킹됩니다. HTTP/2 멀티플렉싱을 적용하면 단일 커넥션으로 수백 개의 스트림을 지원합니다.

---

## 🚀 7. 실행 방법

```bash
# 1. 의존성 설치 및 Wasm 빌드
npm install
npm run wasm:build
cd web && npm install && cd ..

# 2. Rust SSE Mock 서버 실행 (포트 8791)
npm run server:rs

# 3. Next.js 클라이언트 대시보드 실행 (포트 3000)
npm run web

# 4. (선택) Docker Compose 프로덕션 통합 실행
docker compose up --build
```

브라우저에서 **`http://localhost:3000`**으로 접속하여 대시보드 및 Wasm 벤치마크를 확인하세요.
