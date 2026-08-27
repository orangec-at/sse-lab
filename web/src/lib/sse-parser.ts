/**
 * Server-Sent Events (SSE) 메시지 구조체 (W3C/WHATWG 표준 규격)
 */
export interface SSEMessage {
  id?: string;
  event?: string;
  data: string;
  retry?: number;
  comments?: string[];
  raw: string;
}

/**
 * [핵심 엔진] Carry-Buffer 기반 SSE 스트림 파서
 *
 * ============================================================================
 * 1. 왜 Carry Buffer가 필요한가? (TCP 청크와 애플리케이션 프레이밍의 불일치)
 * ============================================================================
 * - TCP는 바이트 스트림 프로토콜입니다. 패킷은 네트워크 혼잡도, OS 커널 소켓 버퍼 크기 등에 따라
 *   아무 위치(예: "da", "ta: ", "hel", "lo\n\nd")에서나 잘려서 도착합니다.
 * - 또한 고속 전송 시 여러 개의 이벤트가 하나의 read() 청크에 합쳐져서 들어오기도 합니다.
 * - 따라서 "남은 불완전한 조각(carry)"을 내부 메모리에 들고 있다가 다음 청크와 이어 붙이는
 *   'Carry 버퍼링'이 필수적입니다.
 *
 * ============================================================================
 * 2. W3C SSE 포맷 사양 처리 규칙
 * ============================================================================
 * - 이벤트 구분자: '\n\n' 또는 '\r\n\r\n' (빈 줄 하나)
 * - 콜론 뒤 공백: "data: hello" -> "hello", "data:hello" -> "hello"
 *   (콜론 뒤 첫 공백 1칸은 프레이밍 공백이므로 데이터가 아님)
 * - 멀티라인: 여러 개의 "data: " 라인은 "\n"으로 결합
 * - 주석: ":"로 시작하는 라인은 하트비트/주석으로 무시
 * - 미종료 스트림: flush()를 통해 소켓이 닫힐 때 버퍼에 남은 마지막 이벤트를 유실 없이 방출
 */
export class SSEParser {
  private carry = '';

  /**
   * 소켓에서 읽어 들인 텍스트 청크를 받아 버퍼에 합치고, 완성된 SSE 이벤트 목록을 반환합니다.
   * @param chunk 새로 수신한 원시 문자열 조각
   */
  feed(chunk: string): SSEMessage[] {
    this.carry += chunk;

    const messages: SSEMessage[] = [];
    
    // Windows 스타일 개행(\r\n)을 유닉스 스타일(\n)으로 표준화
    const normalized = this.carry.replace(/\r\n/g, '\n');
    const parts = normalized.split('\n\n');

    // split 결과의 마지막 요소는 아직 \n\n을 만나지 못한 '미완성 조각'이므로 carry에 보관합니다.
    this.carry = parts.pop() ?? '';

    for (const part of parts) {
      if (part.trim() === '') continue;
      const msg = this.parseBlock(part);
      if (msg) messages.push(msg);
    }

    return messages;
  }

  /**
   * 스트림이 종료(done: true)되었을 때 호출하여,
   * 빈 줄(\n\n) 없이 소켓이 닫혀 carry에 남아있는 마지막 잔여 데이터를 강제 방출합니다.
   */
  flush(): SSEMessage[] {
    if (!this.carry.trim()) {
      this.carry = '';
      return [];
    }
    const msg = this.parseBlock(this.carry.replace(/\r\n/g, '\n'));
    this.carry = '';
    return msg ? [msg] : [];
  }

  /**
   * 파서 상태를 초기화합니다.
   */
  reset() {
    this.carry = '';
  }

  /**
   * 단일 이벤트 블록(\n\n으로 구분된 텍스트 덩어리)을 필드별로 파싱합니다.
   */
  private parseBlock(block: string): SSEMessage | null {
    const lines = block.split('\n');
    const dataLines: string[] = [];
    const comments: string[] = [];
    let event: string | undefined;
    let id: string | undefined;
    let retry: number | undefined;

    for (const line of lines) {
      if (line.startsWith(':')) {
        // 하트비트/주석 라인
        comments.push(line.slice(1).trimStart());
      } else if (line.startsWith('data:')) {
        // "data: " (공백 포함 6바이트) vs "data:" (공백 미포함 5바이트)
        dataLines.push(line.startsWith('data: ') ? line.slice(6) : line.slice(5));
      } else if (line.startsWith('event:')) {
        event = line.startsWith('event: ') ? line.slice(7) : line.slice(6);
      } else if (line.startsWith('id:')) {
        id = line.startsWith('id: ') ? line.slice(4) : line.slice(3);
      } else if (line.startsWith('retry:')) {
        const val = line.startsWith('retry: ') ? line.slice(7) : line.slice(6);
        const parsed = parseInt(val, 10);
        if (!isNaN(parsed)) retry = parsed;
      }
      // 명세상 정의되지 않은 필드(unknown field)는 에러를 내지 않고 조용히 무시해야 함
    }

    // 유의미한 데이터가 전혀 없는 빈 블록은 무시
    if (dataLines.length === 0 && comments.length === 0 && !event && !id) {
      return null;
    }

    return {
      id,
      event,
      data: dataLines.join('\n'), // 멀티라인 데이터는 \n으로 결합
      retry,
      comments: comments.length > 0 ? comments : undefined,
      raw: block,
    };
  }
}
