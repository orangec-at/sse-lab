'use client';

import { useState } from 'react';
import { SSEParser } from '@/lib/sse-parser';
import { initWasm } from '@/lib/wasm-loader';

interface BenchmarkResult {
  engine: 'Pure TypeScript' | 'Rust WebAssembly';
  totalEvents: number;
  elapsedMs: number;
  throughputEventsPerSec: number;
}

export function BenchmarkModal({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const [isRunning, setIsRunning] = useState(false);
  const [eventCount, setEventCount] = useState<number>(20000);
  const [results, setResults] = useState<BenchmarkResult[] | null>(null);

  if (!isOpen) return null;

  const runBenchmark = async () => {
    setIsRunning(true);
    setResults(null);

    // 1. 대용량 SSE 모의 데이터 생성
    const rawEvents: string[] = [];
    for (let i = 0; i < eventCount; i++) {
      rawEvents.push(`id: ${i}\nevent: token\ndata: {"index": ${i}, "content": "Benchmark token payload item ${i}"}\n\n`);
    }
    const fullPayload = rawEvents.join('');

    // 청크 크기를 64바이트로 분할하여 TCP 청크 스트림 시뮬레이션
    const chunkSize = 64;
    const chunks: string[] = [];
    for (let i = 0; i < fullPayload.length; i += chunkSize) {
      chunks.push(fullPayload.slice(i, i + chunkSize));
    }

    // 2. Pure TypeScript Parser 벤치마크 실행
    const tsParser = new SSEParser();
    let tsEventCount = 0;
    const tsStart = performance.now();

    for (const chunk of chunks) {
      const msgs = tsParser.feed(chunk);
      tsEventCount += msgs.length;
    }
    const tsFlushed = tsParser.flush();
    tsEventCount += tsFlushed.length;
    const tsElapsed = performance.now() - tsStart;

    // 3. Rust WebAssembly Parser 벤치마크 실행
    const wasmMod = await initWasm();
    let wasmEventCount = 0;
    let wasmElapsed = 0;

    if (wasmMod) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { WasmSSEParser } = wasmMod as any;
      const wasmParser = new WasmSSEParser();

      const wasmStart = performance.now();
      for (const chunk of chunks) {
        const msgs = wasmParser.feed(chunk);
        if (msgs) wasmEventCount += msgs.length;
      }
      const wasmFlushed = wasmParser.flush();
      if (wasmFlushed) wasmEventCount += wasmFlushed.length;
      wasmElapsed = performance.now() - wasmStart;
    }

    setResults([
      {
        engine: 'Pure TypeScript',
        totalEvents: tsEventCount,
        elapsedMs: Number(tsElapsed.toFixed(2)),
        throughputEventsPerSec: Math.round((tsEventCount / tsElapsed) * 1000),
      },
      {
        engine: 'Rust WebAssembly',
        totalEvents: wasmEventCount,
        elapsedMs: Number(wasmElapsed.toFixed(2)),
        throughputEventsPerSec: Math.round((wasmEventCount / wasmElapsed) * 1000),
      },
    ]);

    setIsRunning(false);
  };

  const speedup =
    results && results[0] && results[1] && results[1].elapsedMs > 0
      ? (results[0].elapsedMs / results[1].elapsedMs).toFixed(2)
      : null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
      <div className="bg-[#161b22] border border-[#30363d] rounded-xl max-w-xl w-full p-6 space-y-5 shadow-2xl text-[#e6edf3]">
        <div className="flex items-center justify-between border-b border-[#30363d] pb-4">
          <div>
            <h2 className="text-lg font-bold text-white flex items-center gap-2">
              ⚡ SSE Parser Benchmark
              <span className="text-xs px-2 py-0.5 rounded bg-purple-900/50 text-purple-300 border border-purple-700/50">
                JS vs Rust Wasm
              </span>
            </h2>
            <p className="text-xs text-[#8b949e] mt-1">
              Simulating parsing throughput over fragmented 64-byte TCP chunks.
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-[#8b949e] hover:text-white text-lg font-mono px-2"
          >
            ✕
          </button>
        </div>

        {/* Configurations */}
        <div className="flex items-center justify-between bg-[#0d1117] p-3 rounded-lg border border-[#262d36] text-xs">
          <span className="text-[#8b949e]">Target Events:</span>
          <div className="flex gap-2">
            {[10000, 25000, 50000].map((count) => (
              <button
                key={count}
                onClick={() => setEventCount(count)}
                disabled={isRunning}
                className={`px-2.5 py-1 rounded font-mono ${
                  eventCount === count
                    ? 'bg-blue-600 text-white font-semibold'
                    : 'bg-[#21262d] text-[#8b949e] hover:text-white'
                }`}
              >
                {count.toLocaleString()}
              </button>
            ))}
          </div>
        </div>

        {/* Run Button */}
        <button
          onClick={runBenchmark}
          disabled={isRunning}
          className="w-full py-2.5 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 text-white font-medium rounded-lg text-sm transition-all disabled:opacity-50"
        >
          {isRunning ? 'Running High-Load Benchmark...' : `Start Benchmark (${eventCount.toLocaleString()} events)`}
        </button>

        {/* Results */}
        {results && (
          <div className="space-y-4 pt-2">
            <div className="grid grid-cols-2 gap-3">
              {results.map((res) => {
                const isWasm = res.engine === 'Rust WebAssembly';
                return (
                  <div
                    key={res.engine}
                    className={`p-4 rounded-lg border ${
                      isWasm
                        ? 'bg-purple-950/20 border-purple-500/50'
                        : 'bg-blue-950/20 border-blue-500/50'
                    }`}
                  >
                    <div className="text-xs font-semibold uppercase tracking-wider text-[#8b949e]">
                      {res.engine}
                    </div>
                    <div className="text-2xl font-mono font-bold text-white mt-1">
                      {res.elapsedMs}
                      <span className="text-xs font-normal text-[#8b949e] ml-1">ms</span>
                    </div>
                    <div className="text-xs font-mono text-emerald-400 mt-2">
                      ⚡ {res.throughputEventsPerSec.toLocaleString()} ev/s
                    </div>
                  </div>
                );
              })}
            </div>

            {speedup && (
              <div className="bg-[#0d1117] border border-emerald-500/30 rounded-lg p-3 text-center text-xs text-emerald-300 font-mono">
                🎉 Rust WebAssembly is <b>{speedup}x</b> faster in raw buffer throughput!
              </div>
            )}
          </div>
        )}

        <div className="text-[11px] text-[#8b949e] leading-relaxed border-t border-[#262d36] pt-3">
          💡 <b>Why WebAssembly wins here:</b> Zero garbage collection (GC) pauses, tightly packed memory buffer allocation, and highly optimized string splitting compiled from native LLVM.
        </div>
      </div>
    </div>
  );
}
