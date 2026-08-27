'use client';

import { useState, useRef, useCallback, useEffect } from 'react';
import { SSEParser, SSEMessage } from '@/lib/sse-parser';
import { initWasm } from '@/lib/wasm-loader';

/** 소켓에서 수신한 원시 청크 데이터 정보 */
export interface RawRead {
  id: number;
  time: number;
  data: string;
}

/** Anthropic 형식의 구조화된 콘텐츠 블록 */
export interface AnthropicBlock {
  type: 'thinking' | 'text' | 'tool_use' | string;
  text: string;
  name?: string;
  done: boolean;
}

/** 실시간 스트리밍 성능 및 네트워크 지표 */
export interface StreamMetrics {
  firstByteMs: number | null; // TTFB (Time To First Byte)
  readsCount: number;         // 소켓 read() 호출 횟수 (청크 수)
  eventsCount: number;        // 파서가 방출한 SSE 이벤트 수
  elapsedMs: number;          // 총 경과 시간(ms)
  parseTimeMs: number;        // 파싱 엔진에 소요된 누적 시간(ms)
}

export type ParserEngine = 'typescript' | 'rust-wasm';
export type StreamStatus = 'idle' | 'streaming' | 'completed' | 'error';

/**
 * [React 커스텀 훅] SSE 스트림 수신, 파싱 및 시각화 상태 관리
 * - Pure TypeScript 파서 및 Rust WebAssembly 파서 전환 지원
 */
export function useSSE() {
  const [status, setStatus] = useState<StreamStatus>('idle');
  const [engine, setEngine] = useState<ParserEngine>('typescript');
  const [wasmReady, setWasmReady] = useState(false);
  const [rawReads, setRawReads] = useState<RawRead[]>([]);
  const [events, setEvents] = useState<SSEMessage[]>([]);
  const [assembledText, setAssembledText] = useState<string>('');
  const [anthropicBlocks, setAnthropicBlocks] = useState<Map<number, AnthropicBlock>>(new Map());
  const [metrics, setMetrics] = useState<StreamMetrics>({
    firstByteMs: null,
    readsCount: 0,
    eventsCount: 0,
    elapsedMs: 0,
    parseTimeMs: 0,
  });

  const abortControllerRef = useRef<AbortController | null>(null);
  const tsParserRef = useRef(new SSEParser());
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const wasmParserInstanceRef = useRef<any>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);

  // Wasm 모듈 초기화
  useEffect(() => {
    initWasm().then((mod) => {
      if (mod) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const { WasmSSEParser } = mod as any;
        wasmParserInstanceRef.current = new WasmSSEParser();
        setWasmReady(true);
      }
    });
  }, []);

  /** 상태 및 버퍼 완전 초기화 */
  const clear = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
    if (timerRef.current) clearInterval(timerRef.current);

    setStatus('idle');
    setRawReads([]);
    setEvents([]);
    setAssembledText('');
    setAnthropicBlocks(new Map());
    setMetrics({
      firstByteMs: null,
      readsCount: 0,
      eventsCount: 0,
      elapsedMs: 0,
      parseTimeMs: 0,
    });
    tsParserRef.current.reset();
    if (wasmParserInstanceRef.current) {
      wasmParserInstanceRef.current.reset();
    }
  }, []);

  /** 스트림 강제 중단 */
  const disconnect = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
    if (timerRef.current) clearInterval(timerRef.current);
    setStatus('completed');
  }, []);

  /**
   * 외부 SSE 서버로 스트리밍 연결 시작
   */
  const connect = useCallback(async (targetUrl: string, useProxy = false, selectedEngine = engine) => {
    clear();

    const controller = new AbortController();
    abortControllerRef.current = controller;
    setStatus('streaming');

    const isWasm = selectedEngine === 'rust-wasm' && wasmParserInstanceRef.current;
    const tsParser = tsParserRef.current;
    const wasmParser = wasmParserInstanceRef.current;
    
    tsParser.reset();
    if (wasmParser) wasmParser.reset();

    const startTime = performance.now();
    let firstByteRecorded = false;
    let readCounter = 0;
    let eventCounter = 0;
    let totalParseTimeMs = 0;
    let currentAssembled = '';
    const currentBlocks = new Map<number, AnthropicBlock>();

    // 50ms 주기로 경과 시간 실시간 갱신
    timerRef.current = setInterval(() => {
      setMetrics((prev) => ({
        ...prev,
        elapsedMs: Math.round(performance.now() - startTime),
      }));
    }, 50);

    try {
      const endpoint = useProxy
        ? `/api/proxy?url=${encodeURIComponent(targetUrl)}`
        : targetUrl;

      const response = await fetch(endpoint, {
        headers: { Accept: 'text/event-stream' },
        signal: controller.signal,
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error('Response body is null');
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const now = performance.now();
        if (!firstByteRecorded) {
          firstByteRecorded = true;
          setMetrics((prev) => ({
            ...prev,
            firstByteMs: Math.round(now - startTime),
          }));
        }

        readCounter++;
        const chunk = decoder.decode(value, { stream: true });

        // 1. 소켓 Raw 청크 기록
        setRawReads((prev) => [
          ...prev,
          { id: readCounter, time: Math.round(now - startTime), data: chunk },
        ]);

        // 2. 파서 실행 및 파싱 소요 시간 측정 (JS vs Rust Wasm)
        const parseStart = performance.now();
        const newEvents: SSEMessage[] = isWasm
          ? wasmParser.feed(chunk)
          : tsParser.feed(chunk);
        totalParseTimeMs += performance.now() - parseStart;

        if (newEvents && newEvents.length > 0) {
          eventCounter += newEvents.length;
          setEvents((prev) => [...prev, ...newEvents]);

          for (const ev of newEvents) {
            if (ev.data === '[DONE]') continue;

            // JSON 페이로드 파싱 (OpenAI / Anthropic 구조 처리)
            try {
              const parsedJson = JSON.parse(ev.data);

              if (parsedJson.type === 'content_block_start') {
                currentBlocks.set(parsedJson.index, {
                  type: parsedJson.content_block.type,
                  name: parsedJson.content_block.name,
                  text: '',
                  done: false,
                });
                setAnthropicBlocks(new Map(currentBlocks));
              } else if (parsedJson.type === 'content_block_delta') {
                const block = currentBlocks.get(parsedJson.index);
                if (block) {
                  const d = parsedJson.delta;
                  block.text += d.text ?? d.thinking ?? d.partial_json ?? '';
                  setAnthropicBlocks(new Map(currentBlocks));
                }
              } else if (parsedJson.type === 'content_block_stop') {
                const block = currentBlocks.get(parsedJson.index);
                if (block) {
                  block.done = true;
                  setAnthropicBlocks(new Map(currentBlocks));
                }
              } else if (parsedJson.choices?.[0]?.delta?.content) {
                currentAssembled += parsedJson.choices[0].delta.content;
                setAssembledText(currentAssembled);
              }
            } catch {
              currentAssembled += ev.data;
              setAssembledText(currentAssembled);
            }
          }
        }

        setMetrics((prev) => ({
          ...prev,
          readsCount: readCounter,
          eventsCount: eventCounter,
          parseTimeMs: Number(totalParseTimeMs.toFixed(3)),
        }));
      }

      // 스트림 종료 시 미종료 tail 플러시
      const flushStart = performance.now();
      const flushed: SSEMessage[] = isWasm ? wasmParser.flush() : tsParser.flush();
      totalParseTimeMs += performance.now() - flushStart;

      if (flushed && flushed.length > 0) {
        eventCounter += flushed.length;
        setEvents((prev) => [...prev, ...flushed]);
        for (const ev of flushed) {
          currentAssembled += ev.data;
        }
        setAssembledText(currentAssembled);
      }

      setMetrics((prev) => ({
        ...prev,
        eventsCount: eventCounter,
        parseTimeMs: Number(totalParseTimeMs.toFixed(3)),
      }));

      setStatus('completed');
    } catch (err: unknown) {
      if ((err as Error).name === 'AbortError') {
        setStatus('completed');
      } else {
        console.error('SSE Stream Error:', err);
        setStatus('error');
      }
    } finally {
      if (timerRef.current) clearInterval(timerRef.current);
      setMetrics((prev) => ({
        ...prev,
        elapsedMs: Math.round(performance.now() - startTime),
      }));
    }
  }, [clear, engine]);

  return {
    status,
    engine,
    setEngine,
    wasmReady,
    rawReads,
    events,
    assembledText,
    anthropicBlocks,
    metrics,
    connect,
    disconnect,
    clear,
  };
}
