use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use flate2::write::GzEncoder;
use flate2::Compression;
use regex::Regex;
use serde_json::json;
use std::{io::Write, sync::LazyLock, time::Duration};
use tokio::time::sleep;
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const PORT: u16 = 8791;
const ANSWER: &str = "Server-Sent Events is a one-way channel: the server writes, the browser reads. \
It rides on a normal HTTP response that never ends, which is why it survives \
proxies and needs no special handshake.";

// 정규식을 사용해 LLM의 토큰 방출 패턴(공백을 포함한 단어 단위)을 흉내냅니다.
static TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s*\S+").unwrap());

fn tokenize(s: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(s)
        .map(|m| m.as_str().to_string())
        .collect()
}

/**
 * [기술적 핵심] SSE 응답 생성 헬퍼 함수
 *
 * 1. Content-Type: text/event-stream; charset=utf-8
 *    - 브라우저 및 클라이언트 파서에게 이 응답이 SSE 프로토콜 스트림임을 알립니다.
 * 2. Cache-Control: no-cache, no-transform
 *    - 중간 프록시(Nginx, Cloudflare 등)가 응답을 캐싱하거나 Gzip 등으로 임의 압축/변형하지 못하게 방지합니다.
 * 3. Connection: keep-alive
 *    - HTTP/1.1에서 연결을 닫지 않고 지속적으로 데이터를 밀어주기 위해 소켓을 유지합니다.
 * 4. X-Accel-Buffering: no
 *    - Nginx의 리버스 프록시 버퍼링(proxy_buffering)을 비활성화하는 핵심 헤더입니다.
 *    - 이 헤더가 없으면 Nginx 기본 설정상 버퍼(보통 4KB~8KB)가 찰 때까지 클라이언트로 토큰이 전달되지 않습니다.
 * 5. Body::from_stream(stream)
 *    - Tokio의 논블로킹 비동기 스트림을 Axum의 응답 바디로 파이프라이닝합니다.
 *    - 고루틴/스레드를 점유하지 않고 커널 epoll/kqueue 이벤트 루프에 등록되어 효율적으로 처리됩니다.
 */
fn sse_response<S>(stream: S) -> Response
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, std::convert::Infallible>>
        + Send
        + 'static,
{
    let mut res = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache, no-transform")
        .header(header::CONNECTION, "keep-alive")
        .header("X-Accel-Buffering", "no")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from_stream(stream))
        .unwrap();

    res.headers_mut()
        .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    res
}

/**
 * 시나리오 1. /plain : 교과서적인 이상적 SSE 스트림
 *
 * - 특징: 각 토큰이 완전한 이벤트 프레이밍(`data: <token>\n\n`)을 갖추고 40ms 간격으로 전송됩니다.
 * - 한계: 실제 네트워크(TCP)는 패킷 경계를 보장하지 않으므로, 실무에서 이 형태로 패킷이 1:1 도착한다고 가정하면 버그가 발생합니다.
 */
async fn plain() -> Response {
    let stream = async_stream::stream! {
        for token in tokenize(ANSWER) {
            let chunk = format!("data: {}\n\n", token);
            yield Ok(bytes::Bytes::from(chunk));
            sleep(Duration::from_millis(40)).await;
        }
        yield Ok(bytes::Bytes::from("data: [DONE]\n\n"));
    };
    sse_response(stream)
}

/**
 * 시나리오 2. /chopped : 임의의 7바이트 단위 절단 스트림 (가장 흔한 파서 버그 재현)
 *
 * [네트워크 계층의 현실]
 * - TCP는 스트림 지향(Stream-oriented) 프로토콜로, 애플리케이션의 메시지 경계를 알지 못합니다.
 * - MTU(Maximum Transmission Unit), Nagle 알고리즘, OS 커널 수신 버퍼 상태에 따라
 *   `data: `라는 필드명 중간에서 잘리거나, 2~3개의 이벤트가 1개의 read() 청크에 뭉쳐서 도착합니다.
 *
 * [실험 목적]
 * - 7바이트씩 강제로 쪼개어 보냄으로써, 수신 측 파서가 'carry 버퍼' 없이 단순 청크 단위 정규식을 쓰면
 *   데이터의 90% 이상이 유실되는 현상을 직접 확인합니다.
 */
async fn chopped() -> Response {
    let mut payload = tokenize(ANSWER)
        .into_iter()
        .map(|t| format!("data: {}\n\n", t))
        .collect::<Vec<_>>()
        .join("");
    payload.push_str("data: [DONE]\n\n");

    let stream = async_stream::stream! {
        let bytes = payload.into_bytes();
        for chunk in bytes.chunks(7) {
            yield Ok(bytes::Bytes::copy_from_slice(chunk));
            sleep(Duration::from_millis(15)).await;
        }
    };
    sse_response(stream)
}

/**
 * 시나리오 3. /quirks : W3C/WHATWG SSE 사양(Spec)의 모든 엣지 케이스
 *
 * 1. `: keep-alive` (주석)
 *    - 콜론(`:`)으로 시작하는 줄은 주석(Comment)입니다.
 *    - 유휴 상태의 TCP 커넥션이 AWS ALB나 프록시 타임아웃으로 끊어지는 것을 방지하는 하트비트(Heartbeat)로 쓰입니다.
 * 2. `data:tight` vs `data: tight` (프레이밍 공백)
 *    - 콜론 뒤 첫 번째 공백 1칸은 구분용 프레이밍이며 데이터가 아닙니다. 둘 다 값은 `"tight"`입니다.
 * 3. `data: line one\ndata: line two` (멀티라인 데이터)
 *    - 여러 개의 `data:` 라인은 `\n`으로 합쳐져 단일 이벤트가 됩니다.
 * 4. `id: 42` 및 `retry: 3000`
 *    - 클라이언트의 `Last-Event-ID`를 갱신하고, 연결 단절 시 재연결 대기 시간(ms)을 브라우저에 지시합니다.
 * 5. `\r\n` (CRLF)
 *    - Windows 계열 프록시나 서버에서 CRLF 개행을 보내더라도 정상 처리되어야 합니다.
 * 6. Unterminated Tail (미종료 꼬리)
 *    - 마지막 데이터 뒤에 빈 줄(`\n\n`) 없이 소켓이 닫힐 때, 파서의 `flush()`가 없으면 마지막 토큰이 조용히 유실됩니다.
 */
async fn quirks() -> Response {
    let stream = async_stream::stream! {
        yield Ok(bytes::Bytes::from(": keep-alive\n\n"));
        sleep(Duration::from_millis(100)).await;

        yield Ok(bytes::Bytes::from("event: greeting\ndata: hello\n\n"));
        sleep(Duration::from_millis(100)).await;

        yield Ok(bytes::Bytes::from("data: line one\ndata: line two\ndata: line three\n\n"));
        sleep(Duration::from_millis(100)).await;

        yield Ok(bytes::Bytes::from("id: 42\nretry: 3000\ndata: with an id\n\n"));
        sleep(Duration::from_millis(100)).await;

        yield Ok(bytes::Bytes::from("data:tight\n\n"));
        sleep(Duration::from_millis(100)).await;

        yield Ok(bytes::Bytes::from("nonsense: ignore me\ndata: survived\n\n"));
        sleep(Duration::from_millis(100)).await;

        yield Ok(bytes::Bytes::from("event: crlf\r\ndata: windows\r\n\r\n"));
        sleep(Duration::from_millis(100)).await;

        // 빈 줄(\n\n) 없이 끝나는 마지막 이벤트
        yield Ok(bytes::Bytes::from("data: unterminated tail"));
    };
    sse_response(stream)
}

/**
 * 시나리오 4. /anthropic : Claude API의 구조화된 인덱스 블록 스트림
 *
 * [아키텍처적 의의]
 * - OpenAI가 하나의 긴 텍스트 스트림을 보내는 것과 달리, Anthropic은 `thinking`, `text`, `tool_use`를
 *   명시적인 인덱스(index)와 블록 생명주기(`content_block_start` -> `delta` -> `stop`)로 전송합니다.
 * - 클라이언트는 마크다운을 추측해서 자를 필요 없이, `content_block_stop` 이벤트를 통해 해당 블록이 확정되었음을 즉시 알 수 있습니다.
 * - `tool_use`의 인자는 파싱 불가능한 JSON 조각(JSON fragment)으로 들어오므로, 마지막에 합쳐서 단 1회 파싱해야 합니다.
 */
async fn anthropic() -> Response {
    let stream = async_stream::stream! {
        let send = |event: &str, data: serde_json::Value| {
            format!("event: {}\ndata: {}\n\n", event, data.to_string())
        };

        yield Ok(bytes::Bytes::from(send("message_start", json!({
            "type": "message_start",
            "message": { "id": "msg_demo", "type": "message", "role": "assistant", "content": [] }
        }))));

        // Block 0: 사고 과정 (Thinking Block)
        yield Ok(bytes::Bytes::from(send("content_block_start", json!({
            "type": "content_block_start", "index": 0, "content_block": { "type": "thinking", "thinking": "" }
        }))));
        for t in tokenize("The user asked what SSE is. Keep it short.") {
            yield Ok(bytes::Bytes::from(send("content_block_delta", json!({
                "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": t }
            }))));
            sleep(Duration::from_millis(30)).await;
        }
        yield Ok(bytes::Bytes::from(send("content_block_stop", json!({
            "type": "content_block_stop", "index": 0
        }))));

        // Block 1: 사용자에게 보이는 실제 텍스트 (Text Block)
        yield Ok(bytes::Bytes::from(send("content_block_start", json!({
            "type": "content_block_start", "index": 1, "content_block": { "type": "text", "text": "" }
        }))));
        for t in tokenize("SSE is a one-way stream of text events over plain HTTP.") {
            yield Ok(bytes::Bytes::from(send("content_block_delta", json!({
                "type": "content_block_delta", "index": 1, "delta": { "type": "text_delta", "text": t }
            }))));
            sleep(Duration::from_millis(30)).await;
        }
        yield Ok(bytes::Bytes::from(send("content_block_stop", json!({
            "type": "content_block_stop", "index": 1
        }))));

        // Block 2: 함수/도구 호출 (Tool Use Block - JSON 파편 스트리밍)
        yield Ok(bytes::Bytes::from(send("content_block_start", json!({
            "type": "content_block_start",
            "index": 2,
            "content_block": { "type": "tool_use", "id": "toolu_demo", "name": "get_weather", "input": {} }
        }))));
        for frag in ["{\"loc", "ation\":", " \"Seo", "ul\"}"] {
            yield Ok(bytes::Bytes::from(send("content_block_delta", json!({
                "type": "content_block_delta", "index": 2, "delta": { "type": "input_json_delta", "partial_json": frag }
            }))));
            sleep(Duration::from_millis(60)).await;
        }
        yield Ok(bytes::Bytes::from(send("content_block_stop", json!({
            "type": "content_block_stop", "index": 2
        }))));

        yield Ok(bytes::Bytes::from(send("message_delta", json!({
            "type": "message_delta", "delta": { "stop_reason": "tool_use" }, "usage": { "output_tokens": 41 }
        }))));
        yield Ok(bytes::Bytes::from(send("message_stop", json!({
            "type": "message_stop"
        }))));
    };
    sse_response(stream)
}

/**
 * 시나리오 5. /openai : OpenAI GPT API의 단일 델타 스트림
 *
 * - 구조: `choices[0].delta.content` 형태의 조각들이 들어오며, 마지막에 `data: [DONE]` 문자열로 스트림이 닫힙니다.
 * - 블록 경계가 없으므로 프론트엔드에서 완성 여부를 추론해야 합니다.
 */
async fn openai() -> Response {
    let stream = async_stream::stream! {
        for t in tokenize("SSE is a one-way stream over plain HTTP.") {
            let payload = json!({
                "choices": [{ "delta": { "content": t } }]
            });
            yield Ok(bytes::Bytes::from(format!("data: {}\n\n", payload.to_string())));
            sleep(Duration::from_millis(40)).await;
        }
        yield Ok(bytes::Bytes::from("data: [DONE]\n\n"));
    };
    sse_response(stream)
}

/**
 * 시나리오 6. /buffered : 압축/미들웨어 버퍼링 함정 재현
 *
 * [기술적 원인]
 * - Gzip 압축 미들웨어나 리버스 프록시가 스트림을 플러시(Flush)하지 않고 끝까지 버퍼에 모아두면,
 *   클라이언트는 1.5초 동안 아무것도 못 받다가(TTFB 급증) 마지막에 한 덩어리로 데이터를 받습니다.
 * - 겉으로는 정상 응답 같지만 '스트리밍의 가치'를 잃어버리게 됩니다.
 */
async fn buffered() -> Response {
    let mut payload = tokenize(ANSWER)
        .into_iter()
        .map(|t| format!("data: {}\n\n", t))
        .collect::<Vec<_>>()
        .join("");
    payload.push_str("data: [DONE]\n\n");

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();

    let stream = async_stream::stream! {
        sleep(Duration::from_millis(1500)).await;
        yield Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(compressed));
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache, no-transform")
        .header(header::CONTENT_ENCODING, "gzip")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from_stream(stream))
        .unwrap()
}

/**
 * 시나리오 7. /truncated : [DONE] 없이 비정상 종료되는 스트림
 *
 * - HTTP 프로토콜 자체에는 "중간에 응답이 비정상으로 끊겼다"는 신호가 없습니다.
 * - 따라서 reader.read()는 에러 없이 { done: true }를 반환하므로,
 *   애플리케이션 계층([DONE] 또는 stop_reason)에서 완료 신호를 반드시 검증해야 합니다.
 */
async fn truncated() -> Response {
    let stream = async_stream::stream! {
        let tokens = tokenize(ANSWER);
        for t in tokens.into_iter().take(6) {
            yield Ok(bytes::Bytes::from(format!("data: {}\n\n", t)));
            sleep(Duration::from_millis(60)).await;
        }
        // [DONE] 없이 즉시 스트림 종료
    };
    sse_response(stream)
}

/**
 * 시나리오 8. /resumable : Last-Event-ID 헤더를 통한 단절 복구
 *
 * - SSE 표준 사양: 연결이 끊기면 클라이언트는 마지막으로 수신한 `id` 값을 `Last-Event-ID` 헤더에 담아 재요청합니다.
 * - 서버는 이 ID 이후의 토큰부터 이어서 전송하여 유실 없이 복구합니다.
 */
async fn resumable(headers: HeaderMap) -> Response {
    let last_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let stream = async_stream::stream! {
        let tokens = tokenize(ANSWER);
        if last_id > 0 {
            yield Ok(bytes::Bytes::from(format!(": resuming after id {}\n\n", last_id)));
        }

        let cutoff = tokens.len() / 3;
        for (i, t) in tokens.into_iter().enumerate().skip(last_id) {
            yield Ok(bytes::Bytes::from(format!("id: {}\ndata: {}\n\n", i + 1, t)));
            sleep(Duration::from_millis(80)).await;

            // 최초 연결 시 1/3 지점에서 강제 연결 종료 (단절 시뮬레이션)
            if i == cutoff && last_id == 0 {
                return;
            }
        }
        yield Ok(bytes::Bytes::from("data: [DONE]\n\n"));
    };
    sse_response(stream)
}

/**
 * 시나리오 9. /duplicating : ID가 없어 재연결 시 중복 수신되는 현상
 *
 * - 실제 대부분의 상용 LLM API는 토큰별 고유 ID를 부여하지 않습니다.
 * - 네트워크 에러로 단순 재시도(Retry)를 하면 처음부터 다시 생성되어 이미 화면에 렌더링된 텍스트가 중복 출력됩니다.
 */
async fn duplicating(headers: HeaderMap) -> Response {
    let is_retry = headers.contains_key("x-retry");
    let stream = async_stream::stream! {
        let tokens = tokenize(ANSWER);
        for (i, t) in tokens.into_iter().enumerate() {
            yield Ok(bytes::Bytes::from(format!("data: {}\n\n", t)));
            sleep(Duration::from_millis(40)).await;
            if i == 5 && !is_retry {
                return;
            }
        }
        yield Ok(bytes::Bytes::from("data: [DONE]\n\n"));
    };
    sse_response(stream)
}

/**
 * 시나리오 10. /firehose : 초고속 스트리밍과 백프레셔(Backpressure)
 *
 * - 3초 동안 지연 없이 최대 속도로 데이터를 밀어냅니다.
 * - 클라이언트의 렌더링 속도가 네트워크 수신 속도를 따라가지 못할 때 발생하는 지연과 부하를 측정합니다.
 */
async fn firehose() -> Response {
    let stream = async_stream::stream! {
        let started = std::time::Instant::now();
        let payload = format!("data: {}\n\n", "x".repeat(512));
        let chunk = bytes::Bytes::from(payload);

        while started.elapsed() < Duration::from_millis(3000) {
            yield Ok(chunk.clone());
            tokio::task::yield_now().await;
        }
    };
    sse_response(stream)
}

const INDEX: &str = r#"SSE lab (Rust Axum) — http://localhost:8791

  /plain       clean framing, one event per token
  /chopped     same events, cut at arbitrary byte offsets
  /quirks      comments, multi-line data, ids, CRLF, unterminated tail
  /anthropic   typed indexed blocks (thinking / text / tool_use)
  /openai      one flat text stream + [DONE] sentinel
  /buffered    gzip without flush — arrives all at once at the end
  /truncated   socket dies mid-stream, no terminator
  /resumable   drops once, resumes via Last-Event-ID
  /firehose    writes flat out; high throughput stream
  /duplicating drops once, no ids — reconnect replays from the top

Try:  curl -N http://localhost:8791/chopped
      curl -N --raw -D - http://localhost:8791/plain
"#;

async fn index() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        INDEX,
    )
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // 웹 브라우저(localhost:3000 등)에서의 직접 Fetch 요청을 위한 CORS 설정
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(index))
        .route("/plain", get(plain))
        .route("/chopped", get(chopped))
        .route("/quirks", get(quirks))
        .route("/anthropic", get(anthropic))
        .route("/openai", get(openai))
        .route("/buffered", get(buffered))
        .route("/truncated", get(truncated))
        .route("/resumable", get(resumable))
        .route("/duplicating", get(duplicating))
        .route("/firehose", get(firehose))
        .layer(cors);

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(PORT);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("{}", INDEX);
    println!("Rust SSE Server listening on http://{}", addr);

    axum::serve(listener, app).await.unwrap();
}
