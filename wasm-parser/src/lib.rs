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
            carry: String::with_capacity(8192),
        }
    }

    pub fn reset(&mut self) {
        self.carry.clear();
    }

    /**
     * [패턴 1: 기본 청크 파싱] 매 청크마다 JS 객체로 변환 (FFI 오버헤드 존재)
     */
    pub fn feed(&mut self, chunk: &str) -> Result<JsValue, JsValue> {
        self.carry.push_str(chunk);

        let normalized = self.carry.replace("\r\n", "\n");
        let parts: Vec<&str> = normalized.split("\n\n").collect();

        if parts.len() <= 1 {
            self.carry = normalized;
            return serde_wasm_bindgen::to_value(&Vec::<WasmSSEMessage>::new())
                .map_err(|e| JsValue::from_str(&e.to_string()));
        }

        let mut messages: Vec<WasmSSEMessage> = Vec::with_capacity(parts.len() - 1);
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
     * [패턴 2: 최적화된 Zero-Copy 바이트 버퍼 피딩]
     * JS의 UTF-8 디코딩 없이 Uint8Array 바이트 슬라이스를 Wasm 선형 메모리로 직접 받아 파싱합니다.
     */
    pub fn feed_bytes(&mut self, bytes: &[u8]) -> Result<JsValue, JsValue> {
        if let Ok(chunk_str) = std::str::from_utf8(bytes) {
            self.feed(chunk_str)
        } else {
            // 멀티바이트 중간 절단 시 처리
            let lossy = String::from_utf8_lossy(bytes);
            self.feed(&lossy)
        }
    }

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
     * [최적화 벤치마크: Micro-Batched 파싱]
     * 여러 개의 작은 청크를 버퍼에 모아서(Batching) FFI 호출 횟수를 1/100로 줄였을 때의 성능 측정
     */
    #[wasm_bindgen]
    pub fn benchmark_batched_run(raw_data: &str, batch_size_bytes: usize) -> u32 {
        let mut parser = WasmSSEParser::new();
        let mut total_events = 0;

        let bytes = raw_data.as_bytes();
        for chunk in bytes.chunks(batch_size_bytes) {
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

        total_events
    }

    #[wasm_bindgen]
    pub fn benchmark_run(raw_data: &str, iterations: u32) -> u32 {
        let mut total_events = 0;
        for _ in 0..iterations {
            total_events += Self::benchmark_batched_run(raw_data, 1024);
        }
        total_events
    }
}
