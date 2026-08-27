'use client';

import { useState, useRef, useCallback, useEffect } from 'react';
import { SSEParser, SSEMessage } from '@/lib/sse-parser';
import { initWasm } from '@/lib/wasm-loader';

export interface RawRead {
  id: number;
  time: number;
  data: string;
}

export interface AnthropicBlock {
  type: 'thinking' | 'text' | 'tool_use' | string;
  text: string;
  name?: string;
  done: boolean;
}

export interface StreamMetrics {
  firstByteMs: number | null;
  readsCount: number;
  eventsCount: number;
  elapsedMs: number;
  parseTimeMs: number;
}

export type ParserEngine = 'typescript' | 'rust-wasm' | 'worker-wasm';
export type StreamStatus = 'idle' | 'streaming' | 'completed' | 'error';

export function useSSE() {
  const [status, setStatus] = useState<StreamStatus>('idle');
  const [engine, setEngine] = useState<ParserEngine>('worker-wasm');
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
  const workerRef = useRef<Worker | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);

  // Web Worker 및 Main Thread Wasm 초기화
  useEffect(() => {
    initWasm().then((mod) => {
      if (mod) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const { WasmSSEParser } = mod as any;
        wasmParserInstanceRef.current = new WasmSSEParser();
        setWasmReady(true);
      }
    });

    if (typeof window !== 'undefined') {
      try {
        const worker = new Worker('/workers/sse-stream.worker.js', { type: 'module' });
        workerRef.current = worker;
      } catch (err) {
        console.warn('Web Worker not supported or failed to create:', err);
      }
    }

    return () => {
      if (workerRef.current) {
        workerRef.current.terminate();
      }
    };
  }, []);

  const clear = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
    if (workerRef.current) {
      workerRef.current.postMessage({ type: 'STOP_STREAM' });
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

  const disconnect = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
    if (workerRef.current) {
      workerRef.current.postMessage({ type: 'STOP_STREAM' });
    }
    if (timerRef.current) clearInterval(timerRef.current);
    setStatus('completed');
  }, []);

  // 메시지 및 블록 조립 헬퍼
  const processNewEvents = useCallback(
    (
      newEvents: SSEMessage[],
      currentBlocks: Map<number, AnthropicBlock>,
      currentAssembledRef: { text: string }
    ) => {
      for (const ev of newEvents) {
        if (ev.data === '[DONE]') continue;

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
            currentAssembledRef.text += parsedJson.choices[0].delta.content;
            setAssembledText(currentAssembledRef.text);
          }
        } catch {
          currentAssembledRef.text += ev.data;
          setAssembledText(currentAssembledRef.text);
        }
      }
    },
    []
  );

  /**
   * 외부 SSE 스트림 연결
   */
  const connect = useCallback(
    async (targetUrl: string, useProxy = false, selectedEngine = engine) => {
      clear();

      const endpoint = useProxy
        ? `/api/proxy?url=${encodeURIComponent(targetUrl)}`
        : targetUrl;

      // =========================================================================
      // [엔진 1] Web Worker + Rust Wasm (Background OS Thread)
      // =========================================================================
      if (selectedEngine === 'worker-wasm' && workerRef.current) {
        setStatus('streaming');
        const worker = workerRef.current;
        const currentBlocks = new Map<number, AnthropicBlock>();
        const currentAssembledRef = { text: '' };

        worker.onmessage = (e) => {
          const msg = e.data;
          if (msg.type === 'FIRST_BYTE') {
            setMetrics((prev) => ({ ...prev, firstByteMs: msg.firstByteMs }));
          } else if (msg.type === 'RAW_READ') {
            setRawReads((prev) => [...prev, msg.read]);
          } else if (msg.type === 'EVENTS') {
            setEvents((prev) => [...prev, ...msg.events]);
            processNewEvents(msg.events, currentBlocks, currentAssembledRef);
            setMetrics((prev) => ({
              ...prev,
              readsCount: msg.readCount,
              eventsCount: msg.eventCount,
              parseTimeMs: msg.parseTimeMs,
              elapsedMs: msg.elapsedMs,
            }));
          } else if (msg.type === 'COMPLETED') {
            setStatus('completed');
            setMetrics((prev) => ({
              ...prev,
              elapsedMs: msg.elapsedMs,
              parseTimeMs: msg.totalParseTimeMs,
            }));
          } else if (msg.type === 'ERROR') {
            console.error('Worker SSE Error:', msg.error);
            setStatus('error');
          }
        };

        // 절대 URL 계산
        const absoluteUrl = endpoint.startsWith('http')
          ? endpoint
          : `${window.location.origin}${endpoint}`;

        worker.postMessage({ type: 'START_STREAM', url: absoluteUrl });
        return;
      }

      // =========================================================================
      // [엔진 2 & 3] Main Thread (Pure TS or Wasm)
      // =========================================================================
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
      const currentAssembledRef = { text: '' };
      const currentBlocks = new Map<number, AnthropicBlock>();

      timerRef.current = setInterval(() => {
        setMetrics((prev) => ({
          ...prev,
          elapsedMs: Math.round(performance.now() - startTime),
        }));
      }, 50);

      try {
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

          setRawReads((prev) => [
            ...prev,
            { id: readCounter, time: Math.round(now - startTime), data: chunk },
          ]);

          const parseStart = performance.now();
          const newEvents: SSEMessage[] = isWasm
            ? wasmParser.feed(chunk)
            : tsParser.feed(chunk);
          totalParseTimeMs += performance.now() - parseStart;

          if (newEvents && newEvents.length > 0) {
            eventCounter += newEvents.length;
            setEvents((prev) => [...prev, ...newEvents]);
            processNewEvents(newEvents, currentBlocks, currentAssembledRef);
          }

          setMetrics((prev) => ({
            ...prev,
            readsCount: readCounter,
            eventsCount: eventCounter,
            parseTimeMs: Number(totalParseTimeMs.toFixed(3)),
          }));
        }

        const flushStart = performance.now();
        const flushed: SSEMessage[] = isWasm ? wasmParser.flush() : tsParser.flush();
        totalParseTimeMs += performance.now() - flushStart;

        if (flushed && flushed.length > 0) {
          eventCounter += flushed.length;
          setEvents((prev) => [...prev, ...flushed]);
          processNewEvents(flushed, currentBlocks, currentAssembledRef);
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
    },
    [clear, engine, processNewEvents]
  );

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
