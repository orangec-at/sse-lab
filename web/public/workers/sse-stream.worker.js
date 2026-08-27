/**
 * [Web Worker] 백그라운드 전용 SSE 스트림 수신 및 Rust Wasm 파싱 엔진
 *
 * 메인 UI 스레드와 완전히 격리된 별도 OS 스레드에서:
 * 1. fetch() 스트림 수신
 * 2. Wasm 선형 메모리 기반 고속 SSE 파싱
 * 3. 메인 스레드로 배치 메시지 전송 (Zero UI Freeze)
 */

/* eslint-disable no-restricted-globals */

let wasmParser = null;
let isWasmReady = false;

// Worker 내부에서 Wasm 모듈 비동기 로드
async function initWorkerWasm() {
  try {
    const wasmModule = await import('/wasm/wasm_parser.js');
    await wasmModule.default('/wasm/wasm_parser_bg.wasm');
    wasmParser = new wasmModule.WasmSSEParser();
    isWasmReady = true;
    self.postMessage({ type: 'WASM_READY' });
  } catch (err) {
    console.error('[Worker] Failed to load Wasm:', err);
    self.postMessage({ type: 'WASM_ERROR', error: String(err) });
  }
}

initWorkerWasm();

let abortController = null;

self.onmessage = async (e) => {
  const { type, url } = e.data;

  if (type === 'START_STREAM') {
    if (abortController) {
      abortController.abort();
    }
    abortController = new AbortController();

    if (!isWasmReady || !wasmParser) {
      await initWorkerWasm();
    }
    wasmParser.reset();

    const startTime = performance.now();
    let firstByteRecorded = false;
    let readCount = 0;
    let eventCount = 0;
    let totalParseTimeMs = 0;

    try {
      const response = await fetch(url, {
        headers: { Accept: 'text/event-stream' },
        signal: abortController.signal,
      });

      if (!response.ok) {
        self.postMessage({ type: 'ERROR', error: `HTTP ${response.status} ${response.statusText}` });
        return;
      }

      if (!response.body) {
        self.postMessage({ type: 'ERROR', error: 'Response body is null' });
        return;
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');

      // 마이크로 배칭 버퍼 (1KB 또는 16ms 프레임 단위)
      let batchBuffer = '';
      const BATCH_SIZE = 1024;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const now = performance.now();
        if (!firstByteRecorded) {
          firstByteRecorded = true;
          self.postMessage({
            type: 'FIRST_BYTE',
            firstByteMs: Math.round(now - startTime),
          });
        }

        readCount++;
        const chunk = decoder.decode(value, { stream: true });

        // 원시 청크 즉시 메인 스레드에 보고
        self.postMessage({
          type: 'RAW_READ',
          read: { id: readCount, time: Math.round(now - startTime), data: chunk },
        });

        batchBuffer += chunk;

        if (batchBuffer.length >= BATCH_SIZE) {
          const parseStart = performance.now();
          const events = wasmParser.feed(batchBuffer);
          totalParseTimeMs += performance.now() - parseStart;

          if (events && events.length > 0) {
            eventCount += events.length;
            self.postMessage({
              type: 'EVENTS',
              events,
              readCount,
              eventCount,
              parseTimeMs: Number(totalParseTimeMs.toFixed(3)),
              elapsedMs: Math.round(performance.now() - startTime),
            });
          }
          batchBuffer = '';
        }
      }

      // 잔여 버퍼 처리
      if (batchBuffer.length > 0) {
        const parseStart = performance.now();
        const events = wasmParser.feed(batchBuffer);
        totalParseTimeMs += performance.now() - parseStart;
        if (events && events.length > 0) {
          eventCount += events.length;
          self.postMessage({
            type: 'EVENTS',
            events,
            readCount,
            eventCount,
            parseTimeMs: Number(totalParseTimeMs.toFixed(3)),
            elapsedMs: Math.round(performance.now() - startTime),
          });
        }
      }

      // Flush
      const flushStart = performance.now();
      const flushed = wasmParser.flush();
      totalParseTimeMs += performance.now() - flushStart;

      if (flushed && flushed.length > 0) {
        eventCount += flushed.length;
        self.postMessage({
          type: 'EVENTS',
          events: flushed,
          readCount,
          eventCount,
          parseTimeMs: Number(totalParseTimeMs.toFixed(3)),
          elapsedMs: Math.round(performance.now() - startTime),
        });
      }

      self.postMessage({
        type: 'COMPLETED',
        elapsedMs: Math.round(performance.now() - startTime),
        totalParseTimeMs: Number(totalParseTimeMs.toFixed(3)),
      });
    } catch (err) {
      if (err.name === 'AbortError') {
        self.postMessage({ type: 'ABORTED' });
      } else {
        self.postMessage({ type: 'ERROR', error: String(err) });
      }
    }
  } else if (type === 'STOP_STREAM') {
    if (abortController) {
      abortController.abort();
      abortController = null;
    }
  }
};
