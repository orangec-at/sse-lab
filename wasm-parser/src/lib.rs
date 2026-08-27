use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/**
 * WebAssembly SSE 메시지 구조체
 */
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WasmSSEMessage {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: String,
    pub retry: Option<u32>,
    pub comments: Option<Vec<String>>,
    pub raw: String,
}

/**
 * [Rust WebAssembly SSE Parser]
 *
 * 브라우저의 JS 메인 스레드 부담을 최소화하고,
 * 대규모 토큰 스트림을 고속으로 Carry 버퍼링 및 파싱하는 Wasm 엔진입니다.
 */
#[wasm_bindgen]
pub struct WasmSSEParser {
    carry: String,
}

#[wasm_bindgen]
impl WasmSSEParser {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            carry: String::with_capacity(4096),
        }
    }

    /**
     * 버퍼를 초기화합니다.
     */
    pub fn reset(&mut self) {
        self.carry.clear();
    }

    /**
     * 새로운 텍스트 청크를 받아 내부 버퍼에 추가하고, 완성된 SSE 이벤트들을 파싱하여 반환합니다.
     */
    pub fn feed(&mut self, chunk: &str) -> Result<JsValue, JsValue> {
        self.carry.push_str(chunk);

        // CRLF 개행을 유닉스 개행으로 일괄 정규화
        let normalized = self.carry.replace("\r\n", "\n");
        let parts: Vec<&str> = normalized.split("\n\n").collect();

        if parts.len() <= 1 {
            // 아직 \n\n 경계가 없으므로 대기
            self.carry = normalized;
            return serde_wasm_bindgen::to_value(&Vec::<WasmSSEMessage>::new())
                .map_err(|e| JsValue::from_str(&e.to_string()));
        }

        let mut messages: Vec<WasmSSEMessage> = Vec::with_capacity(parts.len() - 1);

        // 마지막 요소는 미완성 조각(carry)으로 보관
        let (complete_parts, last_part) = parts.split_at(parts.len() - 1);
        self.carry = last_part[0].to_string();

        for part in complete_parts {
            if part.trim().is_empty() {
                continue;
            }
            if let Some(msg) = Self::parse_block(part) {
                messages.push(msg);
            }
        }

        serde_wasm_bindgen::to_value(&messages).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /**
     * 스트림 종료 시 호출하여 carry에 남아있는 unterminated tail을 방출합니다.
     */
    pub fn flush(&mut self) -> Result<JsValue, JsValue> {
        if self.carry.trim().is_empty() {
            self.carry.clear();
            return serde_wasm_bindgen::to_value(&Vec::<WasmSSEMessage>::new())
                .map_err(|e| JsValue::from_str(&e.to_string()));
        }

        let normalized = self.carry.replace("\r\n", "\n");
        self.carry.clear();

        let mut messages = Vec::new();
        if let Some(msg) = Self::parse_block(&normalized) {
            messages.push(msg);
        }

        serde_wasm_bindgen::to_value(&messages).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /**
     * 단일 SSE 이벤트 블록 파싱 (W3C SSE 사양 완벽 준수)
     */
    fn parse_block(block: &str) -> Option<WasmSSEMessage> {
        let mut data_lines: Vec<&str> = Vec::new();
        let mut comments: Vec<String> = Vec::new();
        let mut event: Option<String> = None;
        let mut id: Option<String> = None;
        let mut retry: Option<u32> = None;

        for line in block.lines() {
            if line.starts_with(':') {
                comments.push(line[1..].trim_start().to_string());
            } else if line.starts_with("data:") {
                let payload = if line.starts_with("data: ") {
                    &line[6..]
                } else {
                    &line[5..]
                };
                data_lines.push(payload);
            } else if line.starts_with("event:") {
                let ev = if line.starts_with("event: ") {
                    &line[7..]
                } else {
                    &line[6..]
                };
                event = Some(ev.to_string());
            } else if line.starts_with("id:") {
                let identifier = if line.starts_with("id: ") {
                    &line[4..]
                } else {
                    &line[3..]
                };
                id = Some(identifier.to_string());
            } else if line.starts_with("retry:") {
                let val_str = if line.starts_with("retry: ") {
                    &line[7..]
                } else {
                    &line[6..]
                };
                if let Ok(num) = val_str.trim().parse::<u32>() {
                    retry = Some(num);
                }
            }
        }

        if data_lines.is_empty() && comments.is_empty() && event.is_none() && id.is_none() {
            return None;
        }

        Some(WasmSSEMessage {
            id,
            event,
            data: data_lines.join("\n"),
            retry,
            comments: if comments.is_empty() {
                None
            } else {
                Some(comments)
            },
            raw: block.to_string(),
        })
    }

    /**
     * [Wasm 벤치마크 함수]
     * 동일한 대용량 청크 데이터를 N회 파싱하고 파싱된 총 이벤트 개수를 반환합니다.
     */
    #[wasm_bindgen]
    pub fn benchmark_run(raw_data: &str, iterations: u32) -> u32 {
        let mut parser = WasmSSEParser::new();
        let mut total_events = 0;

        for _ in 0..iterations {
            parser.reset();
            // 64바이트 단위로 가상 청크 분할 피딩
            for chunk in raw_data.as_bytes().chunks(64) {
                if let Ok(s) = std::str::from_utf8(chunk) {
                    if let Ok(val) = parser.feed(s) {
                        if let Ok(msgs) = serde_wasm_bindgen::from_value::<Vec<WasmSSEMessage>>(val) {
                            total_events += msgs.len() as u32;
                        }
                    }
                }
            }
            if let Ok(val) = parser.flush() {
                if let Ok(msgs) = serde_wasm_bindgen::from_value::<Vec<WasmSSEMessage>>(val) {
                    total_events += msgs.len() as u32;
                }
            }
        }

        total_events
    }
}
