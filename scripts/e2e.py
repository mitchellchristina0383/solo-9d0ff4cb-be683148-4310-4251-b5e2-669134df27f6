#!/usr/bin/env python3
"""端到端自测脚本（仅用标准库）：对运行中的服务栈做完整场景核对。

覆盖：超长丢包二分测量、探测次数对数级、切线即撤建议、断线无法判定、
测量中切线作废、切回原线路也只认切换后的测量、/docs 可用；
窗口对账：开窗/封窗/查账、测量包入账、跨窗迟到回包、封窗报忙。

用法：
    python3 scripts/e2e.py --base http://127.0.0.1:8000
退出码非 0 表示有场景未通过。
"""
from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request

RESULTS: list[tuple[bool, str, str]] = []


def api(base: str, method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        base + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw.decode(errors="replace")


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((ok, name, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def wait_done(base: str, mid: str, timeout_s: float = 60.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        _, rec = api(base, "GET", f"/api/measurements/{mid}")
        if rec["status"] != "running":
            return rec
        time.sleep(0.2)
    raise TimeoutError(f"测量 {mid} 超时未结束")


def measure(base: str, lower: int, upper: int, timeout_ms: int = 300) -> dict:
    status, rec = api(
        base,
        "POST",
        "/api/measure",
        {"lower": lower, "upper": upper, "timeout_ms": timeout_ms, "attempts": 3},
    )
    assert status == 202, f"发起测量失败: {status} {rec}"
    return wait_done(base, rec["id"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--line-a-udp",
        default="127.0.0.1:9001",
        help="线路 a 的 UDP 地址（脚本直接发裸包用）",
    )
    args = parser.parse_args()
    base = args.base.rstrip("/")
    udp_host, udp_port = args.line_a_udp.rsplit(":", 1)
    line_a_udp = (udp_host, int(udp_port))
    print(f"目标服务: {base}  线路 a UDP: {line_a_udp[0]}:{line_a_udp[1]}\n")

    print("== 0. 服务与文档可用 ==")
    status, _ = api(base, "GET", "/")
    check("GET / 返回 200", status == 200)
    req = urllib.request.Request(base + "/docs")
    with urllib.request.urlopen(req, timeout=10) as resp:
        docs_ok = resp.status == 200 and b"swagger" in resp.read().lower()
    check("GET /docs 返回 Swagger UI", docs_ok)

    print("== 1. 初始化两条线路的模拟参数 ==")
    status, cfg_a = api(base, "POST", "/api/lines/a/config",
                        {"max_size": 1500, "delay_ms": 30, "down": False})
    check("线路 a 配置 max_size=1500", status == 200 and cfg_a["max_size"] == 1500)
    status, cfg_b = api(base, "POST", "/api/lines/b/config",
                        {"max_size": 4000, "delay_ms": 80, "down": False})
    check("线路 b 配置 max_size=4000", status == 200 and cfg_b["max_size"] == 4000)
    _, snap = api(base, "GET", "/api/line")
    check("默认激活线路为 a", snap["active_line"] == "a")

    print("== 2. 超长丢包：线路 a 上二分测量 [512, 9000] ==")
    rec = measure(base, 512, 9000)
    check("测量完成", rec["status"] == "completed", rec["detail"])
    check("测出阈值 1500", rec["max_payload"] == 1500,
          f"max_payload={rec['max_payload']}")
    check("探测次数为对数级 (<200，范围宽 8489)", rec["probes_sent"] < 200,
          f"probes_sent={rec['probes_sent']}")
    _, snap = api(base, "GET", "/api/recommendation")
    check("建议已发布 1500",
          snap["recommendation"] and snap["recommendation"]["max_payload"] == 1500)

    print("== 3. 切线：建议立即撤下，换线后重新测量 ==")
    _, snap = api(base, "POST", "/api/line/switch", {"line": "b"})
    check("切到 b 后建议为 null", snap["recommendation"] is None)
    _, snap = api(base, "GET", "/api/recommendation")
    check("撤下立即可见", snap["recommendation"] is None)
    rec = measure(base, 512, 9000)
    check("线路 b 测出阈值 4000", rec["max_payload"] == 4000,
          f"max_payload={rec['max_payload']}")

    print("== 4. 断线：最短包也超时 → 无法判定，本轮不发布建议 ==")
    api(base, "POST", "/api/lines/b/config", {"down": True})
    rec = measure(base, 512, 9000)
    check("状态为 undetermined", rec["status"] == "undetermined", rec["detail"])
    _, snap = api(base, "GET", "/api/recommendation")
    check("旧建议(4000)保留、未发布新建议",
          snap["recommendation"] and snap["recommendation"]["max_payload"] == 4000)
    api(base, "POST", "/api/lines/b/config", {"down": False})

    print("== 5. 测量中切线：旧测量作废，建议立即撤下 ==")
    status, rec = api(base, "POST", "/api/measure",
                      {"lower": 512, "upper": 65507, "timeout_ms": 300, "attempts": 3})
    mid = rec["id"]
    time.sleep(1.0)  # 让测量跑起来
    _, snap = api(base, "POST", "/api/line/switch", {"line": "a"})
    check("切线响应中建议已为 null", snap["recommendation"] is None)
    rec = wait_done(base, mid)
    check("旧测量作废(aborted/discarded)", rec["status"] in ("aborted", "discarded"),
          f"status={rec['status']}")
    _, snap = api(base, "GET", "/api/recommendation")
    check("建议未被旧测量发布", snap["recommendation"] is None)

    print("== 6. 切回原线路：也只接受切换后开始的测量 ==")
    status, rec = api(base, "POST", "/api/measure",
                      {"lower": 512, "upper": 65507, "timeout_ms": 300, "attempts": 3})
    mid = rec["id"]
    time.sleep(1.0)
    api(base, "POST", "/api/line/switch", {"line": "b"})
    api(base, "POST", "/api/line/switch", {"line": "a"})  # 立刻切回 a
    rec = wait_done(base, mid)
    check("切回后旧测量仍作废", rec["status"] in ("aborted", "discarded"),
          f"status={rec['status']}")
    _, snap = api(base, "GET", "/api/recommendation")
    check("建议仍为 null", snap["recommendation"] is None)
    rec = measure(base, 512, 9000)
    check("切回后重新测量有效 (1500)", rec["max_payload"] == 1500)

    print("== 7. 线路统计佐证超长丢包真实发生 ==")
    _, lines = api(base, "GET", "/api/lines")
    by_name = {l["name"]: l for l in lines}
    check("线路 a 有超长丢包计数",
          by_name["a"]["reachable"] and by_name["a"]["stats"]["dropped_oversize"] > 0,
          f"dropped_oversize={by_name['a']['stats']['dropped_oversize']}")
    check("线路 b 有断线丢包计数",
          by_name["b"]["reachable"] and by_name["b"]["stats"]["dropped_down"] > 0,
          f"dropped_down={by_name['b']['stats']['dropped_down']}")

    print("== 8. 窗口对账：开窗→测量发包→查账→封窗 ==")
    status, body = api(base, "POST", "/api/lines/a/windows/open")
    check("开窗返回 201", status == 201, f"status={status}")
    win = body["window"]
    status, _ = api(base, "POST", "/api/lines/a/windows/open")
    check("同一线重复开窗报 409", status == 409, f"status={status}")

    rec = measure(base, 512, 9000)
    status, body = api(base, "GET", f"/api/lines/a/windows/{win['id']}")
    live = body["window"]
    check("测量包已入账（收包>0）", live["received"] > 0,
          f"received={live['received']}")
    check("超长丢弃已入账", live["dropped_oversize"] > 0,
          f"dropped_oversize={live['dropped_oversize']}")
    status, body = api(base, "POST", "/api/lines/a/windows/close")
    settled = body["window"]
    check("封窗即结算", status == 200 and settled["status"] == "settled",
          f"status={settled['status']}, pending={settled['pending_sends']}")
    check("账目自洽（收包=超长+断线+已提交+发送失败）",
          settled["received"] == settled["dropped_oversize"]
          + settled["dropped_down"] + settled["committed_sends"]
          + settled["send_failures"],
          f"received={settled['received']}, committed={settled['committed_sends']}, "
          f"oversize={settled['dropped_oversize']}")
    status, body = api(base, "GET", "/api/lines/a/windows/last")
    check("最近一份结算可取", status == 200 and body["window"]["id"] == win["id"])

    # 封窗后发的包不再记入本轮账
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(b"after-close", line_a_udp)
    sock.close()
    time.sleep(0.1)
    _, body = api(base, "GET", f"/api/lines/a/windows/{win['id']}")
    check("封窗后新包不入旧账", body["window"]["received"] == settled["received"])

    print("== 9. 跨窗迟到回包记回原窗；前一窗结算中封窗报忙 ==")
    api(base, "POST", "/api/lines/a/config", {"delay_ms": 400})
    status, body = api(base, "POST", "/api/lines/a/windows/open")
    first = body["window"]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    sock.sendto(b"late-packet", line_a_udp)
    time.sleep(0.1)  # 包已收下，回包仍在 400ms 延迟队列
    _, body = api(base, "POST", "/api/lines/a/windows/close")
    check("封窗后进入结算（待发=1）",
          body["window"]["status"] == "settling"
          and body["window"]["pending_sends"] == 1,
          f"status={body['window']['status']}, "
          f"pending={body['window']['pending_sends']}")

    status, body = api(base, "POST", "/api/lines/a/windows/open")
    second = body["window"]
    check("前一窗结算中仍可开下一窗", status == 201
          and second["id"] != first["id"])
    sock.sendto(b"next-window", line_a_udp)
    status, body = api(base, "POST", "/api/lines/a/windows/close")
    check("前一窗结算中封下一窗报 409 忙", status == 409,
          f"status={status}, detail={body.get('detail') if isinstance(body, dict) else body}")
    _, body = api(base, "GET", f"/api/lines/a/windows/{second['id']}")
    check("报忙后下一窗仍开放并继续计数",
          body["window"]["status"] == "open" and body["window"]["received"] == 1,
          f"status={body['window']['status']}, "
          f"received={body['window']['received']}")

    got = []
    try:
        while len(got) < 2:
            got.append(sock.recv(2048))
    except socket.timeout:
        pass
    sock.close()
    check("迟到回包确实到达", len(got) == 2, f"收到 {len(got)} 个回包")
    time.sleep(0.1)
    _, body = api(base, "GET", f"/api/lines/a/windows/{first['id']}")
    check("迟到回包记回原窗并结算",
          body["window"]["status"] == "settled"
          and body["window"]["committed_sends"] == 1
          and body["window"]["pending_sends"] == 0,
          f"status={body['window']['status']}, "
          f"committed={body['window']['committed_sends']}")
    _, body = api(base, "GET", f"/api/lines/a/windows/{second['id']}")
    check("下一窗只记自己的回包，未串入原窗迟到包",
          body["window"]["received"] == 1
          and body["window"]["committed_sends"] == 1
          and body["window"]["dropped_oversize"] == 0,
          f"received={body['window']['received']}, "
          f"committed={body['window']['committed_sends']}")
    status, body = api(base, "POST", "/api/lines/a/windows/close")
    check("原窗结算完后下一窗可正常封窗",
          status == 200 and body["window"]["id"] == second["id"],
          f"status={body['window']['status']}")
    api(base, "POST", "/api/lines/a/config", {"delay_ms": 30})
    status, _ = api(base, "GET", "/api/lines/nope/windows")
    check("未知线路查账返回 404", status == 404)

    failed = [item for item in RESULTS if not item[0]]
    print(f"\n汇总: {len(RESULTS) - len(failed)}/{len(RESULTS)} 项通过")
    for _, name, detail in failed:
        print(f"  未通过: {name} {detail}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
