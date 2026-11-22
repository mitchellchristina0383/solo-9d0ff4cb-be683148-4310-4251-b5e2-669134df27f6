"""本地回显线路模拟器：UDP 回显服务 + HTTP 控制口。

可模拟三种线路行为：
- 超长丢包：载荷超过 ``max_size`` 字节的包直接丢弃；
- 回包延迟：收到包后延迟 ``delay_ms`` 毫秒再回显；
- 断线：``down`` 为真时丢弃所有包。

行为以**收到包那一刻**的配置为准（延迟期间改配置不影响已入队的包）。

窗口对账：
- 窗口在**收到包那一刻**归属（延迟回包也记回原窗口），每条线同时只开放一窗；
- 封窗后新包不再记入该窗，待该窗所有待发回包都有结果（发送成功/发送调用失败）
  才结算；
- 前一窗仍在结算时，封下一窗返回 409（忙），下一窗保持开放并继续计数；
- 每条线只保留最近一份已结算汇总，不保存逐包明细。

运行方式：``python -m echo.server``（参数走环境变量，见文件底部 main）。
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.config import MAX_UDP_PAYLOAD


@dataclass
class LineConfig:
    max_size: int = MAX_UDP_PAYLOAD  # 超过即丢（模拟超长丢包）
    delay_ms: int = 0                # 回包延迟
    down: bool = False               # 断线


@dataclass
class LineStats:
    received: int = 0
    echoed: int = 0
    dropped_oversize: int = 0
    dropped_down: int = 0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime | None) -> str | None:
    return None if ts is None else ts.isoformat()


@dataclass
class Window:
    """一窗的汇总账：只记计数，不存逐包明细。"""

    id: str
    line: str
    status: str = "open"  # open/settling/settled
    opened_at: datetime = field(default_factory=_utcnow)
    closed_at: datetime | None = None
    settled_at: datetime | None = None
    received: int = 0
    dropped_oversize: int = 0
    dropped_down: int = 0
    pending_sends: int = 0    # 已收下、尚未得到发送结果的回包
    committed_sends: int = 0  # sendto 调用成功（已提交发送）
    send_failures: int = 0    # sendto 调用抛异常

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "line": self.line,
            "status": self.status,
            "opened_at": _iso(self.opened_at),
            "closed_at": _iso(self.closed_at),
            "settled_at": _iso(self.settled_at),
            "received": self.received,
            "dropped_oversize": self.dropped_oversize,
            "dropped_down": self.dropped_down,
            "pending_sends": self.pending_sends,
            "committed_sends": self.committed_sends,
            "send_failures": self.send_failures,
        }


class WindowBusy(Exception):
    """已有开放窗口，不能再开。"""


class NoOpenWindow(Exception):
    """当前没有开放窗口，无法封窗。"""


class SettlingBusy(Exception):
    """前一窗仍在结算，暂不能封下一窗。"""


class WindowLedger:
    """单条线路的窗口台账：同一时刻最多一窗开放。

    收包时把包记到当时的开放窗口（延迟回包的回调也带着该窗口句柄），
    因此跨窗的迟到回包仍记回原窗。封窗后不再收新包，等本窗待发回包
    全部得到发送结果才结算。只保留最近一份已结算汇总。
    """

    def __init__(self, line: str) -> None:
        self.line = line
        self.open_win: Window | None = None
        self.settling_win: Window | None = None
        self.last_settled: dict | None = None
        self._seq = 0

    def open_window(self) -> Window:
        if self.open_win is not None:
            raise WindowBusy()
        self._seq += 1
        self.open_win = Window(id=f"{self.line}-w{self._seq}", line=self.line)
        return self.open_win

    def close_window(self) -> Window:
        if self.open_win is None:
            raise NoOpenWindow()
        if self.settling_win is not None:
            # 前一窗还在等迟到回包：拒绝封下一窗，下一窗保持开放继续计数。
            raise SettlingBusy()
        win = self.open_win
        win.closed_at = _utcnow()
        win.status = "settling"
        self.open_win = None
        if win.pending_sends == 0:
            self._settle(win)
        else:
            self.settling_win = win
        return win

    def account_receive(self, dropped: str | None = None) -> Window | None:
        """按收包时刻归属窗口；返回需要回包的窗口（需等发送结果）。"""
        win = self.open_win
        if win is None:
            return None
        win.received += 1
        if dropped == "oversize":
            win.dropped_oversize += 1
            return None
        if dropped == "down":
            win.dropped_down += 1
            return None
        win.pending_sends += 1
        return win

    def record_send(self, win: Window | None, ok: bool) -> None:
        """回包的发送结果：成功计入已提交发送，异常计入发送调用失败。"""
        if win is None:
            return
        if ok:
            win.committed_sends += 1
        else:
            win.send_failures += 1
        if win.pending_sends > 0:
            win.pending_sends -= 1
        if win.status == "settling" and win.pending_sends == 0:
            self._settle(win)

    def _settle(self, win: Window) -> None:
        win.status = "settled"
        win.settled_at = _utcnow()
        if self.settling_win is win:
            self.settling_win = None
        self.last_settled = win.to_dict()

    def get(self, window_id: str) -> dict | None:
        for win in (self.open_win, self.settling_win):
            if win is not None and win.id == window_id:
                return win.to_dict()
        if self.last_settled and self.last_settled["id"] == window_id:
            return self.last_settled
        return None

    def view(self) -> dict:
        return {
            "open": None if self.open_win is None else self.open_win.to_dict(),
            "settling": (
                None if self.settling_win is None else self.settling_win.to_dict()
            ),
            "last_settled": self.last_settled,
        }


class EchoProtocol(asyncio.DatagramProtocol):
    """逐字节回显；按收到时刻的配置决定丢包/延迟。"""

    def __init__(
        self,
        cfg: LineConfig,
        stats: LineStats,
        ledger: WindowLedger | None = None,
    ) -> None:
        self.cfg = cfg
        self.stats = stats
        self.ledger = ledger
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport) -> None:  # noqa: ANN001
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.stats.received += 1
        if self.cfg.down:
            self.stats.dropped_down += 1
            if self.ledger is not None:
                self.ledger.account_receive("down")
            return
        if len(data) > self.cfg.max_size:
            self.stats.dropped_oversize += 1
            if self.ledger is not None:
                self.ledger.account_receive("oversize")
            return
        # 窗口归属在收包这一刻确定，延迟回包的闭包也带回这个窗口。
        win = (
            None
            if self.ledger is None
            else self.ledger.account_receive()
        )
        delay_s = self.cfg.delay_ms / 1000
        asyncio.get_running_loop().call_later(delay_s, self._echo, data, addr, win)

    def _echo(self, data: bytes, addr, win: Window | None) -> None:  # noqa: ANN001
        if self.transport is None:
            if self.ledger is not None:
                self.ledger.record_send(win, False)
            return
        try:
            self.transport.sendto(data, addr)
            self.stats.echoed += 1
            if self.ledger is not None:
                self.ledger.record_send(win, True)
        except Exception:  # noqa: BLE001 - 对端消失等，模拟器不因此崩溃
            if self.ledger is not None:
                self.ledger.record_send(win, False)


class ConfigPatch(BaseModel):
    max_size: int | None = Field(default=None, ge=0, le=MAX_UDP_PAYLOAD)
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    down: bool | None = None


def create_echo_app(
    line_name: str,
    udp_host: str,
    udp_port: int,
    max_size: int = MAX_UDP_PAYLOAD,
    delay_ms: int = 0,
    down: bool = False,
) -> FastAPI:
    """构造一条回显线路：控制口随 HTTP 服务启动，UDP 口在 lifespan 内绑定。"""
    cfg = LineConfig(max_size=max_size, delay_ms=delay_ms, down=down)
    stats = LineStats()
    ledger = WindowLedger(line_name)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: EchoProtocol(cfg, stats, ledger),
            local_addr=(udp_host, udp_port),
        )
        app.state.udp_port = transport.get_extra_info("sockname")[1]
        try:
            yield
        finally:
            transport.close()

    app = FastAPI(title=f"回显线路 {line_name}", lifespan=lifespan)
    app.state.cfg = cfg
    app.state.stats = stats
    app.state.ledger = ledger
    app.state.line_name = line_name

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "line": line_name, "udp_port": app.state.udp_port}

    @app.get("/config")
    async def get_config() -> dict:
        return {"line": line_name, **asdict(cfg)}

    @app.post("/config")
    async def set_config(patch: ConfigPatch) -> dict:
        for key, value in patch.model_dump(exclude_none=True).items():
            setattr(cfg, key, value)
        return {"line": line_name, **asdict(cfg)}

    @app.get("/stats")
    async def get_stats() -> dict:
        return {"line": line_name, **asdict(stats)}

    @app.post("/stats/reset")
    async def reset_stats() -> dict:
        stats.received = stats.echoed = 0
        stats.dropped_oversize = stats.dropped_down = 0
        return {"line": line_name, **asdict(stats)}

    @app.get("/windows", summary="窗口台账：当前开放窗、结算中窗、最近一份结算汇总")
    async def list_windows() -> dict:
        return {"line": line_name, **ledger.view()}

    @app.post("/windows/open", status_code=201, summary="开窗")
    async def open_window() -> dict:
        try:
            win = ledger.open_window()
        except WindowBusy:
            raise HTTPException(
                status_code=409,
                detail="本线已有开放窗口，请先封窗（每条线同时只开放一窗）",
            )
        return {"line": line_name, "window": win.to_dict()}

    @app.post("/windows/close", summary="封窗（必要时等待结算）")
    async def close_window() -> dict:
        try:
            win = ledger.close_window()
        except NoOpenWindow:
            raise HTTPException(status_code=409, detail="当前没有开放窗口")
        except SettlingBusy:
            raise HTTPException(
                status_code=409,
                detail="前一窗仍在结算，暂不能封下一窗；下一窗继续计数",
            )
        return {"line": line_name, "window": win.to_dict()}

    @app.get("/windows/last", summary="最近一份结算汇总")
    async def get_last_window() -> dict:
        if ledger.last_settled is None:
            raise HTTPException(status_code=404, detail="尚无已结算窗口")
        return {"line": line_name, "window": ledger.last_settled}

    @app.get("/windows/{window_id}", summary="按窗口查账")
    async def get_window(window_id: str) -> dict:
        win = ledger.get(window_id)
        if win is None:
            raise HTTPException(status_code=404, detail="窗口不存在或已不在保留范围")
        return {"line": line_name, "window": win}

    return app


def main() -> None:
    import uvicorn

    app = create_echo_app(
        line_name=os.environ.get("LINE_NAME", "line"),
        udp_host=os.environ.get("UDP_HOST", "0.0.0.0"),
        udp_port=int(os.environ.get("UDP_PORT", "9001")),
        max_size=int(os.environ.get("MAX_SIZE", str(MAX_UDP_PAYLOAD))),
        delay_ms=int(os.environ.get("DELAY_MS", "0")),
        down=os.environ.get("DOWN", "").lower() in {"1", "true", "yes"},
    )
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("CTRL_PORT", "9101")),
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
