"""窗口对账测试：收包归属、跨窗迟到回包、封窗结算、封窗报忙、发送失败计数。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx

from app.config import LineEndpoint
from app.main import create_app
from echo.server import (
    EchoProtocol,
    LineConfig,
    LineStats,
    SettlingBusy,
    WindowLedger,
)
from tests.echoutils import full_echo

MEASURE_BODY = {"lower": 512, "upper": 9000, "timeout_ms": 200, "attempts": 3}


@asynccontextmanager
async def stack(a_delay_ms=0, a_max_size=1500):
    async with full_echo("a", max_size=a_max_size, delay_ms=a_delay_ms) as ea, full_echo(
        "b", max_size=4000, delay_ms=0
    ) as eb:
        lines = {
            "a": LineEndpoint("a", "127.0.0.1", ea.udp_port, ea.ctrl_url),
            "b": LineEndpoint("b", "127.0.0.1", eb.udp_port, eb.ctrl_url),
        }
        app = create_app(lines)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, ea, eb


class _Collector(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[bytes] = []

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.received.append(data)


class UdpClient:
    """测试用 UDP 客户端：发一个包并可选择等它被回显。"""

    def __init__(self, transport, protocol) -> None:  # noqa: ANN001
        self.transport = transport
        self.protocol = protocol

    @classmethod
    async def create(cls) -> "UdpClient":
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            _Collector, local_addr=("127.0.0.1", 0)
        )
        return cls(transport, protocol)

    def send(self, payload: bytes, port: int) -> None:
        self.transport.sendto(payload, ("127.0.0.1", port))

    async def wait_echo(self, payload: bytes, timeout_s: float = 2.0) -> bool:
        for _ in range(int(timeout_s / 0.01)):
            if payload in self.protocol.received:
                return True
            await asyncio.sleep(0.01)
        return False

    def close(self) -> None:
        self.transport.close()


async def test_ledger_send_failure_and_busy_close():
    """账本级：待发→已提交/发送失败；前一窗结算中封下一窗报忙，下一窗继续计数。"""
    ledger = WindowLedger("x")
    first = ledger.open_window()

    kept = ledger.account_receive()  # 正常包，待发
    ledger.account_receive("oversize")
    ledger.account_receive("down")
    ledger.record_send(kept, True)
    assert first.received == 3
    assert first.dropped_oversize == 1
    assert first.dropped_down == 1
    assert first.committed_sends == 1
    assert first.pending_sends == 0

    late = ledger.account_receive()  # 另一个包，仍待发
    assert ledger.close_window().status == "settling"
    assert first.pending_sends == 1

    second = ledger.open_window()  # 前一窗结算中仍可开下一窗
    try:
        ledger.close_window()
    except SettlingBusy:
        pass
    else:  # pragma: no cover - 未报忙即失败
        raise AssertionError("前一窗结算中封下一窗应报忙")
    # 报忙后下一窗保持开放并继续计数
    assert ledger.open_win is second
    win2_pkt = ledger.account_receive()
    assert second.received == 1

    ledger.record_send(late, False)  # 发送调用失败 → 前一窗结算
    assert first.status == "settled"
    assert first.send_failures == 1
    assert ledger.settling_win is None
    assert ledger.last_settled["id"] == first.id
    assert ledger.get(first.id)["committed_sends"] == 1

    # 最近一份结算保留，更早的窗口查不到
    ledger.record_send(win2_pkt, True)
    assert ledger.close_window().status == "settled"
    assert ledger.last_settled["id"] == second.id
    assert ledger.get(first.id) is None


async def test_protocol_counts_send_failure():
    """回显协议：transport.sendto 抛异常时记发送调用失败，不再挂待发。"""
    ledger = WindowLedger("x")
    win = ledger.open_window()
    proto = EchoProtocol(LineConfig(delay_ms=0), LineStats(), ledger)

    class BrokenTransport:
        def sendto(self, data, addr):  # noqa: ANN001
            raise OSError("模拟发送失败")

    proto.transport = BrokenTransport()  # type: ignore[assignment]
    proto.datagram_received(b"hello", ("127.0.0.1", 1))
    await asyncio.sleep(0.01)
    assert win.received == 1
    assert win.send_failures == 1
    assert win.committed_sends == 0
    assert win.pending_sends == 0


async def test_api_window_accounting_and_drop_reasons():
    """API 全链路：开窗→发包（正常/超长）→查账→封窗→新包不入旧账。"""
    async with stack(a_max_size=1500) as (client, ea, _):
        resp = await client.post("/api/lines/a/windows/open")
        assert resp.status_code == 201
        win_id = resp.json()["window"]["id"]

        # 重复开窗：每条线同时只开放一窗
        assert (await client.post("/api/lines/a/windows/open")).status_code == 409

        udp = await UdpClient.create()
        try:
            ok_payload, big_payload = b"ok-" * 40, b"x" * 2000
            udp.send(ok_payload, ea.udp_port)
            assert await udp.wait_echo(ok_payload)
            udp.send(big_payload, ea.udp_port)  # 超长丢包
            await asyncio.sleep(0.1)
        finally:
            udp.close()

        body = (await client.get(f"/api/lines/a/windows/{win_id}")).json()
        win = body["window"]
        assert win["received"] == 2
        assert win["committed_sends"] == 1
        assert win["dropped_oversize"] == 1
        assert win["dropped_down"] == 0
        assert win["pending_sends"] == 0

        closed = (await client.post("/api/lines/a/windows/close")).json()["window"]
        assert closed["status"] == "settled"
        assert closed["committed_sends"] == 1

        # 封窗后新包不再记入这轮账
        udp = await UdpClient.create()
        try:
            udp.send(b"stray", ea.udp_port)
            await asyncio.sleep(0.1)
        finally:
            udp.close()
        assert (
            await client.get(f"/api/lines/a/windows/{win_id}")
        ).json()["window"]["received"] == 2

        last = (await client.get("/api/lines/a/windows/last")).json()["window"]
        assert last["id"] == win_id and last["received"] == 2

        # 没有开放窗时封窗、查不存在的窗口
        assert (await client.post("/api/lines/a/windows/close")).status_code == 409
        assert (await client.get("/api/lines/a/windows/nope")).status_code == 404
        assert (await client.get("/api/lines/nope/windows")).status_code == 404


async def test_api_cross_window_delayed_reply_and_busy_close():
    """延迟回包在下一窗开放后才发出，仍记回原窗；前一窗结算中封下一窗报忙。"""
    async with stack(a_delay_ms=300) as (client, ea, _):
        first = (await client.post("/api/lines/a/windows/open")).json()["window"]

        udp = await UdpClient.create()
        try:
            payload = b"late-reply-check"
            udp.send(payload, ea.udp_port)
            await asyncio.sleep(0.05)  # 包已收下，回包还在延迟队列里

            closed = (
                await client.post("/api/lines/a/windows/close")
            ).json()["window"]
            assert closed["status"] == "settling"
            assert closed["pending_sends"] == 1
            assert closed["committed_sends"] == 0

            # 上一窗未结算，仍可开下一窗；下一窗开始计数
            second = (
                await client.post("/api/lines/a/windows/open")
            ).json()["window"]
            assert second["id"] != first["id"]

            # 前一窗结算中封下一窗 → 409 忙；下一窗保持开放并继续计数
            busy = await client.post("/api/lines/a/windows/close")
            assert busy.status_code == 409
            assert "结算" in busy.json()["detail"]
            udp.send(b"next-window", ea.udp_port)
            await asyncio.sleep(0.05)
            still_open = (
                await client.get(f"/api/lines/a/windows/{second['id']}")
            ).json()["window"]
            assert still_open["status"] == "open"
            assert still_open["received"] == 1

            # 等迟到回包发出：记回原窗，原窗结算
            assert await udp.wait_echo(payload)
            await asyncio.sleep(0.05)
            first_now = (
                await client.get(f"/api/lines/a/windows/{first['id']}")
            ).json()["window"]
            assert first_now["status"] == "settled"
            assert first_now["committed_sends"] == 1
            assert first_now["pending_sends"] == 0
            second_now = (
                await client.get(f"/api/lines/a/windows/{second['id']}")
            ).json()["window"]
            assert second_now["received"] == 1
            assert second_now["committed_sends"] == 0

            # 原窗结算完，下一窗自身还有 300ms 延迟回包；等它也有结果才结算
            assert await udp.wait_echo(b"next-window")
            await asyncio.sleep(0.05)
            closed2 = (
                await client.post("/api/lines/a/windows/close")
            ).json()["window"]
            assert closed2["id"] == second["id"]
            assert closed2["status"] == "settled"
            assert closed2["committed_sends"] == 1
            assert (
                await client.get("/api/lines/a/windows/last")
            ).json()["window"]["id"] == second["id"]
        finally:
            udp.close()


async def test_measure_packets_land_in_open_window():
    """用现有测量接口发包，窗口账能看到测量期间的真实收包/发送计数。"""
    async with stack() as (client, ea, _):
        win = (await client.post("/api/lines/a/windows/open")).json()["window"]
        resp = await client.post("/api/measure", json=MEASURE_BODY)
        assert resp.status_code == 202
        mid = resp.json()["id"]
        for _ in range(400):
            rec = (await client.get(f"/api/measurements/{mid}")).json()
            if rec["status"] != "running":
                break
            await asyncio.sleep(0.05)
        assert rec["status"] == "completed"

        closed = (await client.post("/api/lines/a/windows/close")).json()["window"]
        assert closed["id"] == win["id"]
        assert closed["received"] >= 3  # 测量确实打了包
        assert closed["dropped_oversize"] >= 1  # 二分越界包被超长丢弃
        assert closed["committed_sends"] >= 1
        assert closed["pending_sends"] == 0
        assert (
            closed["received"]
            == closed["dropped_oversize"]
            + closed["dropped_down"]
            + closed["committed_sends"]
            + closed["send_failures"]
        )
