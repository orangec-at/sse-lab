'use client';

import { useState } from 'react';
import { SSEParser } from '@/lib/sse-parser';
import { initWasm } from '@/lib/wasm-loader';

interface BenchmarkMetrics {
  name: string;
  totalEvents: number;
  elapsedMs: number;
  opsPerSec: number;
  memoryEstimate?: string;
  desc: string;
}

export function BenchmarkModal({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const [isRunning, setIsRunning] = useState(false);
  const [eventCount, setEventCount] = useState<number>(30000);
  const [chunkSize, setChunkSize] = useState<number>(64);
  const [iterations, setIterations] = useState<number>(3);
  const [metrics, setMetrics] = useState<BenchmarkMetrics[] | null>(null);

  if (!isOpen) return null;

  const runBenchmark = async () => {
    setIsRunning(true);
    setMetrics(null);

    // 브라우저 렌더링 틱 확보
    await new Promise((r) => setTimeout(r, 50));

    // 1. 대용량 SSE 모의 데이터 생성
    const rawEvents: string[] = [];
    for (let i = 0; i < eventCount; i++) {
      rawEvents.push(`id: ${i}\nevent: message\ndata: {"token_id": ${i}, "payload": "Sample token stream item ${i}"}\n\n`);
    }
    const fullPayload = rawEvents.join('');

    // 청크 단위 분할 (TCP 패킷 단편화 재현)
    const chunks: string[] = [];
    for (let i = 0; i < fullPayload.length; i += chunkSize) {
      chunks.push(fullPayload.slice(i, i + chunkSize));
    }

    const wasmMod = await initWasm();

    // -------------------------------------------------------------
    // 테스트 1: Pure TypeScript Parser (V8 JIT)
    // -------------------------------------------------------------
    let tsTotalTime = 0;
    let tsEventCount = 0;

    for (let iter = 0; iter < iterations; iter++) {
      const tsParser = new SSEParser();
      let count = 0;
      const start = performance.now();
      for (const chunk of chunks) {
        const msgs = tsParser.feed(chunk);
        count += msgs.length;
      }
      count += tsParser.flush().length;
      tsTotalTime += performance.now() - start;
      tsEventCount = count;
    }
    const tsAvgTime = tsTotalTime / iterations;

    // -------------------------------------------------------------
    // 테스트 2: Rust WebAssembly (Per-Chunk FFI Bridge)
    // -------------------------------------------------------------
    let wasmBridgeTime = 0;
    let wasmBridgeCount = 0;

    if (wasmMod) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { WasmSSEParser } = wasmMod as any;
      for (let iter = 0; iter < iterations; iter++) {
        const wasmParser = new WasmSSEParser();
        let count = 0;
        const start = performance.now();
        for (const chunk of chunks) {
          const msgs = wasmParser.feed(chunk);
          if (msgs) count += msgs.length;
        }
        const flushed = wasmParser.flush();
        if (flushed) count += flushed.length;
        wasmBridgeTime += performance.now() - start;
        wasmBridgeCount = count;
      }
    }
    const wasmBridgeAvgTime = wasmBridgeTime / iterations;

    // -------------------------------------------------------------
    // 테스트 3: Rust WebAssembly (Native Linear Memory Bulk Execution)
    // -------------------------------------------------------------
    let wasmBulkTime = 0;
    let wasmBulkCount = 0;

    if (wasmMod) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { WasmSSEParser } = wasmMod as any;
      const start = performance.now();
      wasmBulkCount = WasmSSEParser.benchmark_run(fullPayload, iterations) / iterations;
      wasmBulkTime = (performance.now() - start) / iterations;
    }

    setMetrics([
      {
        name: 'Pure JavaScript (V8 Engine)',
        totalEvents: tsEventCount,
        elapsedMs: Number(tsAvgTime.toFixed(2)),
        opsPerSec: Math.round((tsEventCount / tsAvgTime) * 1000),
        desc: 'V8 JIT 최적화 적용. 단, 대량 파싱 시 가비지 컬렉션(GC) 힙 객체 급증.',
      },
      {
        name: 'Rust Wasm (Per-Chunk FFI Bridge)',
        totalEvents: wasmBridgeCount,
        elapsedMs: Number(wasmBridgeAvgTime.toFixed(2)),
        opsPerSec: Math.round((wasmBridgeCount / wasmBridgeAvgTime) * 1000),
        desc: '매 청크마다 JS ↔ Wasm 경계를 넘나드는 FFI 직렬화(Serialization) 오버헤드 포함.',
      },
      {
        name: 'Rust Wasm (Pure Linear Memory Bulk)',
        totalEvents: wasmBulkCount,
        elapsedMs: Number(wasmBulkTime.toFixed(2)),
        opsPerSec: Math.round((wasmBulkCount / wasmBulkTime) * 1000),
        desc: 'Wasm 선형 메모리 내부에서 제로 카피로 처리. FFI 경계 비용 없는 Rust 순수 연산 속도.',
      },
    ]);

    setIsRunning(false);
  };

  const maxOps = metrics ? Math.max(...metrics.map((m) => m.opsPerSec)) : 1;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-md p-4 overflow-y-auto">
      <div className="bg-[#161b22] border border-[#30363d] rounded-2xl max-w-2xl w-full p-6 space-y-6 shadow-2xl text-[#e6edf3] my-8">
        {/* Modal Header */}
        <div className="flex items-center justify-between border-b border-[#30363d] pb-4">
          <div>
            <div className="flex items-center gap-2.5">
              <h2 className="text-xl font-bold text-white flex items-center gap-2">
                ⚡ Stream Parser Performance Benchmark
              </h2>
              <span className="text-[11px] px-2 py-0.5 rounded-full bg-purple-900/50 text-purple-300 border border-purple-700/50 font-mono">
                V8 JIT vs Rust Wasm
              </span>
            </div>
            <p className="text-xs text-[#8b949e] mt-1">
              정밀 측정: TCP 청크 단편화 환경에서 V8 JIT 엔진과 WebAssembly FFI 바운더리 비용 실시간 비교
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-[#8b949e] hover:text-white text-xl font-mono px-2 rounded hover:bg-[#21262d]"
          >
            ✕
          </button>
        </div>

        {/* Configurations Controls */}
        <div className="grid grid-cols-3 gap-3 bg-[#0d1117] p-3.5 rounded-xl border border-[#262d36] text-xs">
          <div>
            <span className="text-[#8b949e] block mb-1.5 font-medium">Event Count:</span>
            <div className="flex gap-1.5">
              {[10000, 30000, 50000].map((c) => (
                <button
                  key={c}
                  onClick={() => setEventCount(c)}
                  disabled={isRunning}
                  className={`px-2 py-1 rounded font-mono text-[11px] flex-1 ${
                    eventCount === c
                      ? 'bg-blue-600 text-white font-semibold'
                      : 'bg-[#21262d] text-[#8b949e] hover:text-white'
                  }`}
                >
                  {(c / 1000).toFixed(0)}k
                </button>
              ))}
            </div>
          </div>

          <div>
            <span className="text-[#8b949e] block mb-1.5 font-medium">Chunk Size (TCP):</span>
            <div className="flex gap-1.5">
              {[32, 64, 256].map((s) => (
                <button
                  key={s}
                  onClick={() => setChunkSize(s)}
                  disabled={isRunning}
                  className={`px-2 py-1 rounded font-mono text-[11px] flex-1 ${
                    chunkSize === s
                      ? 'bg-purple-600 text-white font-semibold'
                      : 'bg-[#21262d] text-[#8b949e] hover:text-white'
                  }`}
                >
                  {s}B
                </button>
              ))}
            </div>
          </div>

          <div>
            <span className="text-[#8b949e] block mb-1.5 font-medium">Warmup Iterations:</span>
            <div className="flex gap-1.5">
              {[1, 3, 5].map((it) => (
                <button
                  key={it}
                  onClick={() => setIterations(it)}
                  disabled={isRunning}
                  className={`px-2 py-1 rounded font-mono text-[11px] flex-1 ${
                    iterations === it
                      ? 'bg-emerald-600 text-white font-semibold'
                      : 'bg-[#21262d] text-[#8b949e] hover:text-white'
                  }`}
                >
                  {it}x Avg
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Run Button */}
        <button
          onClick={runBenchmark}
          disabled={isRunning}
          className="w-full py-3 bg-gradient-to-r from-blue-600 via-purple-600 to-emerald-600 hover:opacity-90 text-white font-semibold rounded-xl text-sm transition-all disabled:opacity-50 shadow-lg"
        >
          {isRunning
            ? 'Running Multi-Iteration Benchmark...'
            : `Run Benchmark (${eventCount.toLocaleString()} events × ${iterations} iterations)`}
        </button>

        {/* Visualized Results Bar Chart */}
        {metrics && (
          <div className="space-y-4 pt-2">
            <div className="space-y-3">
              {metrics.map((m, idx) => {
                const percent = Math.round((m.opsPerSec / maxOps) * 100);
                const color =
                  idx === 0
                    ? 'bg-blue-500'
                    : idx === 1
                    ? 'bg-purple-500'
                    : 'bg-emerald-500';

                return (
                  <div
                    key={m.name}
                    className="bg-[#0d1117] border border-[#262d36] rounded-xl p-4 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <span className="text-xs font-bold text-white block">{m.name}</span>
                        <span className="text-[11px] text-[#8b949e]">{m.desc}</span>
                      </div>
                      <div className="text-right">
                        <div className="text-base font-mono font-bold text-white">
                          {m.elapsedMs} <span className="text-xs text-[#8b949e]">ms</span>
                        </div>
                        <div className="text-xs font-mono font-semibold text-emerald-400">
                          {m.opsPerSec.toLocaleString()} ev/s
                        </div>
                      </div>
                    </div>

                    {/* Progress Bar Visualization */}
                    <div className="w-full bg-[#21262d] h-2.5 rounded-full overflow-hidden">
                      <div
                        className={`h-full ${color} transition-all duration-700 ease-out`}
                        style={{ width: `${percent}%` }}
                      ></div>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Deep Engineering Analysis & Takeaways */}
            <div className="bg-[#161b22] border border-[#30363d] rounded-xl p-4 space-y-2 text-xs text-[#c9d1d9]">
              <div className="font-bold text-white flex items-center gap-1.5">
                🧠 심층 분석: 왜 이런 결과가 나오는가? (Engineering Insights)
              </div>
              <ul className="space-y-1.5 text-[11px] text-[#8b949e] leading-relaxed list-disc list-inside">
                <li>
                  <b className="text-[#e6edf3]">FFI 바운더리 비용 (Boundary Cost):</b> 매 64바이트 작은 청크마다 JS ↔ Wasm 함수를 호출하면, <code>serde_wasm_bindgen</code>의 직렬화 오버헤드가 순수 파싱 시간보다 커질 수 있습니다.
                </li>
                <li>
                  <b className="text-[#e6edf3]">V8 JIT vs Wasm의 장단점:</b> V8 JIT는 단순 JS 루프를 극도로 최적화하지만 <b>대량 객체 생성 시 GC Stop-The-World</b>가 발생합니다. 반면 Rust Wasm은 <b>GC가 없으며 선형 메모리 일괄 처리 시 최고 속도</b>를 보장합니다.
                </li>
                <li>
                  <b className="text-[#e6edf3]">실무 아키텍처 결론:</b> 고속 스트리밍에서는 작은 청크마다 Wasm을 오가지 않고, 버퍼를 모아서 일괄 처리하거나 Web Worker 내에서 Wasm을 상주시키는 것이 최적의 패턴입니다.
                </li>
              </ul>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
