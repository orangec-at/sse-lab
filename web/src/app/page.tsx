'use client';

import { useState } from 'react';
import { useSSE } from '@/hooks/useSSE';

const ROUTES: Record<string, { desc: string; label: string }> = {
  chopped: {
    label: '/chopped',
    desc: 'The same events, written in 7-byte pieces. Watch how few reads align with event boundaries — this is why parsers need carry buffers.',
  },
  plain: {
    label: '/plain',
    desc: 'One event per token, cleanly framed. The ideal case that tutorials show.',
  },
  anthropic: {
    label: '/anthropic',
    desc: 'Typed indexed blocks: thinking, text, and tool_use JSON fragments. content_block_stop signals completion explicitly.',
  },
  openai: {
    label: '/openai',
    desc: 'One flat text stream ending in [DONE]. No block boundaries.',
  },
  quirks: {
    label: '/quirks',
    desc: 'Spec edge cases: comments (:), named events, multi-line data, Last-Event-ID, CRLF, and unterminated tails.',
  },
  buffered: {
    label: '/buffered',
    desc: 'Gzip without flush. Every byte arrives at once at the end. Check TTFB vs total elapsed time.',
  },
  truncated: {
    label: '/truncated',
    desc: 'The socket dies mid-answer with no [DONE] terminator.',
  },
  resumable: {
    label: '/resumable',
    desc: 'Drops once, then resumes seamlessly via Last-Event-ID header.',
  },
  duplicating: {
    label: '/duplicating',
    desc: 'Drops once without event IDs. Reconnecting replays the entire stream from the beginning.',
  },
  firehose: {
    label: '/firehose',
    desc: 'High-throughput token stream. Tests client rendering performance and backpressure.',
  },
};

export default function Home() {
  const [baseUrl, setBaseUrl] = useState('http://localhost:8791');
  const [currentRoute, setCurrentRoute] = useState('chopped');
  const [useProxy, setUseProxy] = useState(false);

  const {
    status,
    rawReads,
    events,
    assembledText,
    anthropicBlocks,
    metrics,
    connect,
    disconnect,
    clear,
  } = useSSE();

  const handleRun = () => {
    const fullUrl = `${baseUrl.replace(/\/+$/, '')}/${currentRoute}`;
    connect(fullUrl, useProxy);
  };

  const isAnthropic = currentRoute === 'anthropic' || anthropicBlocks.size > 0;

  return (
    <div className="min-h-screen bg-[#0d1117] text-[#e6edf3] font-sans antialiased p-6 md:p-10">
      <main className="max-w-6xl mx-auto space-y-6">
        {/* Header */}
        <header className="border-b border-[#262d36] pb-6 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold tracking-tight text-white">SSE Lab</h1>
              <span className="px-2.5 py-0.5 text-xs font-semibold rounded-full bg-blue-900/50 text-blue-400 border border-blue-700/50">
                Next.js Consumer + Rust Server
              </span>
            </div>
            <p className="text-sm text-[#8b949e] mt-1">
              Visualizing how Server-Sent Events behave over raw TCP socket streams and LLM outputs.
            </p>
          </div>

          {/* Target SSE Server Configuration */}
          <div className="flex flex-wrap items-center gap-3 bg-[#161b22] border border-[#262d36] p-2.5 rounded-lg text-xs">
            <div className="flex items-center gap-2">
              <span className="text-[#8b949e]">Server URL:</span>
              <input
                type="text"
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
                className="bg-[#0d1117] border border-[#30363d] rounded px-2.5 py-1 text-[#e6edf3] font-mono text-xs w-48 focus:outline-none focus:border-blue-500"
              />
            </div>
            <label className="flex items-center gap-1.5 cursor-pointer text-[#8b949e] hover:text-white">
              <input
                type="checkbox"
                checked={useProxy}
                onChange={(e) => setUseProxy(e.target.checked)}
                className="rounded border-[#30363d] bg-[#0d1117] text-blue-500 focus:ring-0"
              />
              Next.js Proxy (BFF)
            </label>
          </div>
        </header>

        {/* Route Selector */}
        <div className="space-y-3">
          <div className="text-xs uppercase tracking-wider font-semibold text-[#8b949e]">
            Select Stream Scenario
          </div>
          <div className="flex flex-wrap gap-2">
            {Object.entries(ROUTES).map(([key, item]) => {
              const active = currentRoute === key;
              return (
                <button
                  key={key}
                  onClick={() => setCurrentRoute(key)}
                  className={`px-3 py-1.5 rounded-md text-xs font-mono transition-colors ${
                    active
                      ? 'bg-blue-600 text-white font-semibold shadow-sm'
                      : 'bg-[#161b22] border border-[#262d36] text-[#c9d1d9] hover:border-[#8b949e]'
                  }`}
                >
                  {item.label}
                </button>
              );
            })}
          </div>
          <div className="bg-[#161b22] border-l-4 border-blue-500 border-y border-r border-[#262d36] rounded-r-md px-4 py-2.5 text-xs text-[#8b949e]">
            {ROUTES[currentRoute].desc}
          </div>
        </div>

        {/* Control Bar & Actions */}
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <button
              onClick={handleRun}
              disabled={status === 'streaming'}
              className="px-5 py-2 rounded-md font-medium text-sm bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-all"
            >
              Run Stream
            </button>
            <button
              onClick={disconnect}
              disabled={status !== 'streaming'}
              className="px-4 py-2 rounded-md font-medium text-sm bg-[#21262d] hover:bg-[#30363d] text-[#c9d1d9] border border-[#30363d] disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Stop
            </button>
            <button
              onClick={clear}
              className="px-4 py-2 rounded-md font-medium text-sm bg-transparent hover:bg-[#21262d] text-[#8b949e] hover:text-white border border-[#262d36]"
            >
              Clear
            </button>
          </div>

          <div className="flex items-center gap-2 text-xs">
            <span className="text-[#8b949e]">Status:</span>
            <span
              className={`font-semibold uppercase tracking-wider ${
                status === 'streaming'
                  ? 'text-emerald-400 animate-pulse'
                  : status === 'completed'
                  ? 'text-blue-400'
                  : status === 'error'
                  ? 'text-rose-400'
                  : 'text-[#8b949e]'
              }`}
            >
              {status}
            </span>
          </div>
        </div>

        {/* Metrics Bar */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="bg-[#161b22] border border-[#262d36] rounded-lg p-3.5">
            <div className="text-[11px] uppercase tracking-wider font-semibold text-[#8b949e]">
              First Byte (TTFB)
            </div>
            <div className="text-xl font-bold text-white mt-1 font-mono">
              {metrics.firstByteMs !== null ? `${metrics.firstByteMs}ms` : '—'}
            </div>
          </div>
          <div className="bg-[#161b22] border border-[#262d36] rounded-lg p-3.5">
            <div className="text-[11px] uppercase tracking-wider font-semibold text-[#8b949e]">
              Socket Reads (Chunks)
            </div>
            <div className="text-xl font-bold text-amber-400 mt-1 font-mono">
              {metrics.readsCount}
            </div>
          </div>
          <div className="bg-[#161b22] border border-[#262d36] rounded-lg p-3.5">
            <div className="text-[11px] uppercase tracking-wider font-semibold text-[#8b949e]">
              Parsed Events
            </div>
            <div className="text-xl font-bold text-emerald-400 mt-1 font-mono">
              {metrics.eventsCount}
            </div>
          </div>
          <div className="bg-[#161b22] border border-[#262d36] rounded-lg p-3.5">
            <div className="text-[11px] uppercase tracking-wider font-semibold text-[#8b949e]">
              Elapsed Time
            </div>
            <div className="text-xl font-bold text-white mt-1 font-mono">
              {metrics.elapsedMs}
              <span className="text-xs text-[#8b949e] font-normal ml-1">ms</span>
            </div>
          </div>
        </div>

        {/* Realtime Stream Panes: Raw Chunks vs Parsed Events */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Left Pane: Raw Reads */}
          <div className="bg-[#161b22] border border-[#262d36] rounded-lg overflow-hidden flex flex-col">
            <div className="px-4 py-2.5 border-b border-[#262d36] flex items-center justify-between text-xs font-semibold text-[#8b949e] uppercase tracking-wider">
              <span>Raw Reads From Socket</span>
              <span className="text-[10px] text-amber-400/80 font-mono">TCP / Chunked</span>
            </div>
            <div className="p-3 h-80 overflow-y-auto font-mono text-xs space-y-1 bg-[#0d1117]/50">
              {rawReads.length === 0 && (
                <div className="text-[#484f58] italic py-8 text-center">No socket reads yet.</div>
              )}
              {rawReads.map((read) => (
                <div key={read.id} className="leading-relaxed hover:bg-[#161b22] px-1.5 py-0.5 rounded">
                  <span className="text-[#8b949e] mr-2">[{read.id.toString().padStart(3, '0')}]</span>
                  <span className="text-amber-300">{JSON.stringify(read.data)}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Right Pane: Parsed Events */}
          <div className="bg-[#161b22] border border-[#262d36] rounded-lg overflow-hidden flex flex-col">
            <div className="px-4 py-2.5 border-b border-[#262d36] flex items-center justify-between text-xs font-semibold text-[#8b949e] uppercase tracking-wider">
              <span>Events After Carry Buffering</span>
              <span className="text-[10px] text-emerald-400/80 font-mono">\n\n Framed</span>
            </div>
            <div className="p-3 h-80 overflow-y-auto font-mono text-xs space-y-1 bg-[#0d1117]/50">
              {events.length === 0 && (
                <div className="text-[#484f58] italic py-8 text-center">No parsed events yet.</div>
              )}
              {events.map((ev, idx) => (
                <div key={idx} className="leading-relaxed hover:bg-[#161b22] px-1.5 py-0.5 rounded border-l-2 border-emerald-500/50 pl-2">
                  {ev.event && <span className="text-blue-400 font-semibold mr-2">event: {ev.event}</span>}
                  {ev.id && <span className="text-purple-400 font-semibold mr-2">id: {ev.id}</span>}
                  {ev.comments && <span className="text-[#8b949e] mr-2">: {ev.comments.join(', ')}</span>}
                  <span className="text-emerald-300">
                    {ev.data ? `data: ${JSON.stringify(ev.data)}` : ''}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Assembled Output Viewer */}
        <div className="bg-[#161b22] border border-[#262d36] rounded-lg overflow-hidden">
          <div className="px-4 py-2.5 border-b border-[#262d36] flex items-center justify-between text-xs font-semibold text-[#8b949e] uppercase tracking-wider">
            <span>Assembled Output</span>
            <span className="text-[10px] text-blue-400 font-mono">
              {isAnthropic ? 'Structured Blocks' : 'Stream Text'}
            </span>
          </div>
          <div className="p-4 min-h-28 text-sm">
            {isAnthropic ? (
              <div className="space-y-3">
                {Array.from(anthropicBlocks.entries()).map(([index, block]) => (
                  <div key={index} className="border border-[#30363d] rounded-lg p-3 bg-[#0d1117]">
                    <div className="flex items-center justify-between text-xs font-mono text-[#8b949e] mb-2">
                      <span className="font-semibold text-white uppercase">
                        [{index}] {block.type} {block.name ? `(${block.name})` : ''}
                      </span>
                      <span className={block.done ? 'text-emerald-400' : 'text-amber-400'}>
                        {block.done ? 'Done' : 'Streaming...'}
                      </span>
                    </div>
                    {block.type === 'thinking' && (
                      <div className="text-xs text-amber-200/90 italic bg-amber-950/20 border-l-2 border-amber-500/50 p-2.5 rounded font-mono">
                        {block.text || 'Thinking...'}
                      </div>
                    )}
                    {block.type === 'text' && (
                      <div className="text-[#e6edf3] whitespace-pre-wrap leading-relaxed">
                        {block.text || '...'}
                      </div>
                    )}
                    {block.type === 'tool_use' && (
                      <div className="bg-[#161b22] p-2.5 rounded font-mono text-xs text-purple-300 overflow-x-auto">
                        <pre>{block.text}</pre>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <div className="font-mono text-xs whitespace-pre-wrap leading-relaxed text-[#e6edf3]">
                {assembledText || <span className="text-[#484f58] italic">Stream output will appear here...</span>}
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
