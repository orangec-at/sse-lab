/**
 * WebAssembly 모듈 비동기 로더 및 초기화 헬퍼
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let wasmInitPromise: Promise<any> | null = null;

export async function initWasm() {
  if (typeof window === 'undefined') return null;

  if (!wasmInitPromise) {
    wasmInitPromise = (async () => {
      try {
        // 브라우저 런타임에서 public/wasm/wasm_parser.js 로드
        // eslint-disable-next-line @typescript-eslint/no-implied-eval
        const importDynamic = new Function('modulePath', 'return import(modulePath)');
        const wasmModule = await importDynamic('/wasm/wasm_parser.js');
        await wasmModule.default('/wasm/wasm_parser_bg.wasm');
        return wasmModule;
      } catch (err) {
        console.error('Failed to initialize WebAssembly parser:', err);
        return null;
      }
    })();
  }

  return wasmInitPromise;
}
