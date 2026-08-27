'use client';

import { useState, useRef, useCallback } from 'react';
import { SSEParser, SSEMessage } from '@/lib/sse-parser';

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
}

export type StreamStatus = 'idle' | 'streaming' | 'completed' | 'error';

/**
 * [React 커스텀 훅] SSE 스트림 수신, 파싱 및 시각화 상태 관리
 *
 * [주요 기술적 처리]
 * 1. Fetch API + ReadableStream (`response.body.getReader()`)
 *    - 브라우저 표준 비동기 스트림을 사용하여 메모리 낭비 없이 청크를 즉시 소비합니다.
 * 2. TextDecoder({ stream: true })
 *    - 멀티바이트 UTF-8 문자(한글, 이모지 등)가 청크 경계에서 바이트 단위로 잘렸을 때 깨짐 없이 복원합니다.
 * 3. SSEParser와의 연동
 *    - 원시 청크 수신 즉시 UI에 기록함과 동시에 Carry 버퍼 파서로 넘겨 완전한 이벤트 단위로 조립합니다.
 * 4. LLM 프로바이더별 구조 파싱
 *    - Anthropic의 블록 생명주기(start/delta/stop) 및 Tool JSON 조각 결합 처리.
 *    - OpenAI의 Choices Delta 처리.
 */
export function useSSE() {
  const [status, setStatus] = useState<StreamStatus>('idle');
  const [rawReads, setRawReads] = useState<RawRead[]>([]);
  const [events, setEvents] = useState<SSEMessage[]>([]);
  const [assembledText, setAssembledText] = useState<string>('');
  const [anthropicBlocks, setAnthropicBlocks] = useState<Map<number, AnthropicBlock>>(new Map());
  const [metrics, setMetrics] = useState<StreamMetrics>({
    firstByteMs: null,
    readsCount: 0,
    eventsCount: 0,
    elapsedMs: 0,
  });

  const abortControllerRef = useRef<AbortController | null>(null);
  const parserRef = useRef(new SSEParser());
  const timerRef = useRef<NodeJS.Timeout | null>(null);

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
    });
    parserRef.current.reset();
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
   * @param targetUrl 대상 SSE 엔드포인트 URL
   * @param useProxy Next.js 서버를 통한 프록시(BFF) 경유 여부
   */
  const connect = useCallback(async (targetUrl: string, useProxy = false) => {
    clear();

    const controller = new AbortController();
    abortControllerRef.current = controller;
    setStatus('streaming');

    const parser = parserRef.current;
    parser.reset();

    const startTime = performance.now();
    let firstByteRecorded = false;
    let readCounter = 0;
    let eventCounter = 0;
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
      // { stream: true } 옵션이 없으면 멀티바이트 문자가 잘렸을 때 (U+FFFD)로 깨집니다.
      const decoder = new TextDecoder('utf-8');

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const now = performance.now();
        // 첫 번째 바이트가 도착한 시점(TTFB) 측정
        if (!firstByteRecorded) {
          firstByteRecorded = true;
          setMetrics((prev) => ({
            ...prev,
            firstByteMs: Math.round(now - startTime),
          }));
        }

        readCounter++;
        const chunk = decoder.decode(value, { stream: true });

        // 1. 소켓 날것의 청크(Raw Read) 기록
        setRawReads((prev) => [
          ...prev,
          { id: readCounter, time: Math.round(now - startTime), data: chunk },
        ]);

        // 2. Carry 버퍼 파서를 통한 완성된 SSE 이벤트 추출
        const newEvents = parser.feed(chunk);

        if (newEvents.length > 0) {
          eventCounter += newEvents.length;
          setEvents((prev) => [...prev, ...newEvents]);

          for (const ev of newEvents) {
            if (ev.data === '[DONE]') continue;

            // JSON 페이로드 파싱 (OpenAI / Anthropic 구조 처리)
            try {
              const parsedJson = JSON.parse(ev.data);

              // Anthropic 구조: index 기반 typed content block
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
                // OpenAI 구조: choices delta content
                currentAssembled += parsedJson.choices[0].delta.content;
                setAssembledText(currentAssembled);
              }
            } catch {
              // 일반 텍스트 스트림
              currentAssembled += ev.data;
              setAssembledText(currentAssembled);
            }
          }
        }

        setMetrics((prev) => ({
          ...prev,
          readsCount: readCounter,
          eventsCount: eventCounter,
        }));
      }

      // 소켓 종료 시 미종료 잔여 데이터 플러시
      const flushed = parser.flush();
      if (flushed.length > 0) {
        eventCounter += flushed.length;
        setEvents((prev) => [...prev, ...flushed]);
        for (const ev of flushed) {
          currentAssembled += ev.data;
        }
        setAssembledText(currentAssembled);
        setMetrics((prev) => ({
          ...prev,
          eventsCount: eventCounter,
        }));
      }

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
  }, [clear]);

  return {
    status,
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
