# main.py
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import io
import json
import uuid
import time
import anyio
import urllib.request
import urllib.error
import websocket as pyws  # pip install websocket-client
import comfyuiservice

# =========================
# Settings
# =========================
COMFY_ADDR = "127.0.0.1:8188"
HEARTBEAT_INTERVAL = 15          # WS keepalive ping 주기(초)
COMFY_PROMPT_TIMEOUT = 30        # /prompt HTTP 요청 타임아웃(초)
PROGRESS_OVERALL_TIMEOUT = 1800  # 진행률 WS 전체 작업 타임아웃(초) = 30분

app = FastAPI()

# CORS (운영시 특정 도메인으로 제한 권장)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # 개발 중에는 전체 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Helpers
# =========================
def queue_prompt_to_comfy(prompt: dict, client_id: str) -> dict:
    """
    ComfyUI /prompt 로 프롬프트 제출하고 JSON 응답을 반환.
    실패 시 RuntimeError 발생(상위에서 처리).
    """
    payload = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"http://{COMFY_ADDR}/prompt",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=COMFY_PROMPT_TIMEOUT) as resp:
            body = resp.read()
            if not body:
                raise RuntimeError("Empty response from ComfyUI /prompt")
            try:
                j = json.loads(body)
            except Exception:
                # JSON 파싱 실패 시 일부 본문 로그
                raise RuntimeError(f"Non-JSON response from /prompt: {body[:300]!r}")
            if "prompt_id" not in j:
                raise RuntimeError(f"Missing 'prompt_id' in /prompt response: {j}")
            return j
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", "ignore")
        raise RuntimeError(f"HTTPError {e.code} from /prompt: {err}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URLError contacting ComfyUI /prompt: {e.reason}") from e


def open_comfy_ws(client_id: str) -> pyws.WebSocket:
    """
    ComfyUI /ws 에 연결한 websocket-client 인스턴스 반환.
    내부 수신 타임아웃을 설정(유휴 시 예외 유도 → 루프 지속).
    """
    w = pyws.WebSocket()
    w.settimeout(30)  # 수신 대기 타임아웃
    w.connect(f"ws://{COMFY_ADDR}/ws?clientId={client_id}")
    return w


# =========================
# HTTP: 이미지 스트리밍
# =========================
@app.get("/get-image")
async def get_image(input: str = Query(..., min_length=1)):
    """
    프롬프트를 받아 comfyuiservice.fetch_image_from_comfy 로 생성된 이미지를 PNG로 스트리밍.
    """
    # comfyuiservice.fetch_image_from_comfy 는 동기 함수라고 가정
    # 오래 걸릴 수 있으니 스레드 오프로딩도 고려 가능(anyio.to_thread.run_sync)
    try:
        image_bytes = comfyuiservice.fetch_image_from_comfy(input)
        if not image_bytes:
            # 내부 로직상 None/빈 바이트일 경우
            return StreamingResponse(io.BytesIO(b""), media_type="image/png", status_code=502)
        return StreamingResponse(io.BytesIO(image_bytes), media_type="image/png")
    except Exception as e:
        # 운영 시에는 구조적 로깅 권장
        return StreamingResponse(io.BytesIO(b""), media_type="image/png", status_code=500)


# =========================
# WS: 진행률 중계(프락시)
# =========================
@app.websocket("/ws/progress")
async def ws_progress(ws: WebSocket, input: str = Query(..., min_length=1)):
    """
    Unity는 ComfyUI에 직접 연결하지 않고 이 엔드포인트에만 연결.
    - 서버가 ComfyUI에 프롬프트 제출(/prompt) 후,
    - ComfyUI /ws 메시지를 수신하여 progress/executing/done 만 Unity로 중계
    - 주기적으로 ping 전송해 연결 유지
    """
    await ws.accept()
    client_id = str(uuid.uuid4())

    # 1) 워크플로우 생성 (사용자 정의)
    prompt = comfyuiservice.get_prompt_with_workflow(input)

    # 2) /prompt 제출
    try:
        resp = queue_prompt_to_comfy(prompt, client_id)
        prompt_id = resp["prompt_id"]
    except Exception as e:
        await ws.send_json({"type": "error", "message": str(e)})
        await ws.close()
        return

    # 3) ComfyUI WS 수신 펌프
    async def pump_messages():
        w = await anyio.to_thread.run_sync(open_comfy_ws, client_id)
        try:
            while True:
                try:
                    msg = await anyio.to_thread.run_sync(w.recv)  # str or bytes
                except Exception:
                    # ComfyUI WS 유휴 타임아웃 → 계속 대기
                    continue

                if isinstance(msg, str):
                    try:
                        payload = json.loads(msg)
                    except Exception:
                        continue

                    t = payload.get("type")
                    data = payload.get("data", {}) or {}

                    if t == "progress":
                        val, mx = data.get("value", 0), data.get("max", 1)
                        pct = (float(val) / float(mx) * 100.0) if mx else 0.0
                        await ws.send_json({
                            "type": "progress",
                            "value": val,
                            "max": mx,
                            "percent": round(pct, 1)
                        })

                    elif t == "executing":
                        node_id = data.get("node")
                        # node_id는 문자열 키이므로 prompt에서 바로 조회 가능
                        class_type = prompt.get(node_id, {}).get("class_type") if node_id else None
                        await ws.send_json({
                            "type": "executing",
                            "node": node_id,
                            "class_type": class_type
                        })

                        # 전체 실행 종료
                        if data.get("prompt_id") == prompt_id and node_id is None:
                            await ws.send_json({"type": "done"})
                            break
                    # 그 외 타입은 무시하거나 필요 시 전달
                # 바이너리 프레임(이미지)은 여기선 무시(이미지는 /get-image로 받음)
        finally:
            try:
                w.close()
            except Exception:
                pass

    # 4) Keepalive heartbeat
    async def heartbeat():
        while True:
            await anyio.sleep(HEARTBEAT_INTERVAL)
            try:
                await ws.send_json({"type": "ping", "ts": time.time()})
            except Exception:
                break  # 연결이 끊기면 종료

    # 5) 동시에 실행 + 전체 타임아웃 관리
    try:
        with anyio.move_on_after(PROGRESS_OVERALL_TIMEOUT) as scope:
            async with anyio.create_task_group() as tg:
                tg.start_soon(pump_messages)
                tg.start_soon(heartbeat)
        if scope.cancel_called:
            await ws.send_json({"type": "error", "message": "progress timeout"})
    except WebSocketDisconnect:
        # 클라이언트가 끊은 경우
        pass
    except Exception as e:
        try:
            await ws.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass

# (참고) Uvicorn 실행 예:
# uvicorn main:app --host 0.0.0.0 --port 8000 --ws websockets --ws-ping-interval 20 --ws-ping-timeout 20
