import { NextRequest } from 'next/server';

/**
 * [Next.js Route Handler] SSE Streaming Reverse Proxy (BFF 패턴)
 *
 * ============================================================================
 * 1. 왜 프록시(BFF)가 필요한가?
 * ============================================================================
 * - 보안(Security): 브라우저에 노출되면 안 되는 LLM API Key나 인증 토큰을 숨길 수 있습니다.
 * - CORS 우회: 외부 서버가 CORS를 허용하지 않더라도 서버 간 통신으로 우회할 수 있습니다.
 * - 스트림 가공: 외부 스트림을 사용자에게 전달하기 전에 토큰 검열, 포맷 변환 등을 적용할 수 있습니다.
 *
 * ============================================================================
 * 2. 기술적 핵심: ReadableStream Passthrough
 * ============================================================================
 * - `upstreamRes.body`는 Node.js 메모리에 전체를 버퍼링하지 않고, 웹 표준 `ReadableStream` 형태로
 *   클라이언트에게 즉시 바이너리 파이프라이닝(Zero-buffering pipeline)됩니다.
 * - `X-Accel-Buffering: no` 및 `Cache-Control: no-cache, no-transform`을 필수 지정합니다.
 */

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const targetUrl = searchParams.get('url');

  if (!targetUrl) {
    return new Response('Missing "url" query parameter', { status: 400 });
  }

  try {
    const upstreamRes = await fetch(targetUrl, {
      headers: {
        Accept: 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
    });

    if (!upstreamRes.ok) {
      return new Response(`Upstream error: ${upstreamRes.statusText}`, {
        status: upstreamRes.status,
      });
    }

    if (!upstreamRes.body) {
      return new Response('No response body from upstream', { status: 502 });
    }

    return new Response(upstreamRes.body, {
      headers: {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : 'Unknown proxy error';
    return new Response(`Proxy error: ${message}`, { status: 500 });
  }
}
