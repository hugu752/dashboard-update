"""
订单流量化分析看板 v2 - FastAPI 后端
=====================================
架构: FastAPI + WebSocket + 前端轻量级图表库
"""

import json
import os
import re
import time
import ast
import asyncio
import threading
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import requests

# ============================================================================
# 配置
# ============================================================================

MCP_SSE_URL = "http://127.0.0.1:3000/sse"
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_state.json")
HOST = "127.0.0.1"
PORT = 8501
KLINE_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kline_cache")
os.makedirs(KLINE_CACHE_DIR, exist_ok=True)

app = FastAPI(title="订单流量化分析看板 v2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ============================================================================
# 持久化 MCP SSE 客户端
# ============================================================================

class MCPClient:
    """持久化MCP SSE连接 - 避免每次请求重新建立SSE连接"""

    def __init__(self, url=MCP_SSE_URL):
        self.url = url
        self.endpoint_url = None
        self._resp = None
        self._listen_thread = None
        self._responses = {}
        self._lock = threading.Lock()
        self._connected = False
        self._id_counter = 1000
        self._subscribed = set()
        self._connect_lock = threading.Lock()
        self._last_reconnect = 0

    def _next_id(self):
        self._id_counter += 1
        return self._id_counter

    def connect(self, force=False):
        """建立SSE连接并握手。幂等操作，已连接时直接返回。"""
        with self._connect_lock:
            if self._connected and not force:
                return True
            try:
                # 关闭旧连接
                if self._resp:
                    try:
                        self._resp.close()
                    except:
                        pass
                self._resp = None
                self._connected = False

                self._resp = requests.get(self.url, stream=True, timeout=40,
                                          headers={"Accept": "text/event-stream"})
                ep_state = {"ep": None}
                responses_tmp = {}
                local_lock = threading.Lock()

                def listen():
                    et = "message"
                    dl = []
                    try:
                        for line in self._resp.iter_lines(decode_unicode=True):
                            if line.startswith("event:"):
                                et = line[6:].strip()
                            elif line.startswith("data:"):
                                dl.append(line[5:].strip())
                            elif line == "":
                                if dl:
                                    raw = "\n".join(dl)
                                    if et == "endpoint":
                                        ep_state["ep"] = raw
                                    elif et == "message":
                                        try:
                                            d = json.loads(raw)
                                            if "id" in d:
                                                with local_lock:
                                                    responses_tmp[d["id"]] = d
                                                with self._lock:
                                                    self._responses[d["id"]] = d
                                        except:
                                            pass
                                    et = "message"
                                    dl = []
                    except Exception as e:
                        print(f"MCP SSE listen error: {e}")
                    # 连接断开
                    self._connected = False

                self._listen_thread = threading.Thread(target=listen, daemon=True)
                self._listen_thread.start()

                # 等待endpoint事件
                for _ in range(50):
                    if ep_state["ep"]:
                        break
                    time.sleep(0.1)

                if not ep_state["ep"]:
                    self._resp.close()
                    return False

                ep = ep_state["ep"]
                self.endpoint_url = f"http://127.0.0.1:3000{ep}" if ep.startswith("/") else ep

                # 握手
                requests.post(self.endpoint_url, json={
                    "jsonrpc": "2.0", "id": 0, "method": "initialize",
                    "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                               "clientInfo": {"name": "Dashboard", "version": "2.0"}}
                }, timeout=5)

                # 等待initialize响应
                for _ in range(50):
                    with local_lock:
                        if 0 in responses_tmp:
                            break
                    time.sleep(0.05)

                requests.post(self.endpoint_url, json={
                    "jsonrpc": "2.0", "method": "notifications/initialized"
                }, timeout=5)

                self._connected = True
                self._last_reconnect = time.time()
                # 不清空_subscribed - 重连后需要重新订阅
                self._need_resubscribe = True
                return True
            except Exception as e:
                print(f"MCP connect error: {e}")
                return False

    def _post(self, method, params, rid):
        """发送JSON-RPC请求(fire-and-forget, 不阻塞等待结果)"""
        if not self.endpoint_url:
            return False
        try:
            # 用短超时发送POST - 服务器会保持连接直到工具执行完毕
            # 但我们不等, 结果通过SSE流返回
            requests.post(self.endpoint_url, json={
                "jsonrpc": "2.0", "id": rid, "method": method,
                "params": params
            }, timeout=(0.5, 0.5))
            return True
        except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout):
            # 超时是正常的 - 服务器在处理请求, 结果会从SSE流回来
            return True
        except Exception as e:
            print(f"MCP POST error: {e}")
            self._connected = False
            return False

    def call_tool(self, name, arguments, timeout=15):
        """调用MCP工具并等待响应。返回原始content或None。"""
        if not self._connected:
            if not self.connect():
                return None
        rid = self._next_id()
        if not self._post("tools/call", {"name": name, "arguments": arguments}, rid):
            return None

        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if rid in self._responses:
                    resp = self._responses.pop(rid)
                    content = resp.get("result", {}).get("content", [])
                    return content
            time.sleep(0.05)
        return None  # timeout

    def call_tool_parsed(self, name, arguments, timeout=15):
        """调用MCP工具并返回解析后的结果(用于交易)"""
        content = self.call_tool(name, arguments, timeout)
        if content is None:
            return {"success": False, "error": "MCP调用超时"}
        parsed = _mcp_parse(content)
        return {"success": True, "result": parsed}

    def ensure_subscribed(self, instruments):
        """确保所有合约已订阅行情推送"""
        need_sub = []
        for inst in instruments:
            iid = inst["instrument_id"]
            if iid not in self._subscribed:
                need_sub.append(inst)

        if not need_sub:
            return True

        for inst in need_sub:
            rid = self._next_id()
            self._post("tools/call", {
                "name": "start_quote_feed",
                "arguments": {"exchange": inst["exchange"], "instrument_id": inst["instrument_id"]}
            }, rid)
            time.sleep(0.03)
            self._subscribed.add(inst["instrument_id"])

        # 等待订阅生效
        time.sleep(0.8)
        return True

    # MCP style参数 → 数据key映射
    _STYLE_KEY_MAP = {"M1": "1m", "M5": "5m", "M15": "15m", "D1": "1d"}

    def fetch_data(self, instruments, full=False):
        """批量获取tick+K线数据(顺序调用，可靠但较慢)。用于手动刷新。"""
        if not instruments:
            return {}
        if not self._connected:
            if not self.connect():
                return {}

        self.ensure_subscribed(instruments)

        if full:
            candle_cfg = [("M1", 120), ("M5", 60), ("M15", 48), ("D1", 30)]
        else:
            candle_cfg = [("M1", 15), ("M5", 8), ("M15", 8), ("D1", 5)]

        result = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            ex = inst["exchange"]
            result[iid] = {}

            # tick
            content = self.call_tool("fetch_tick", {"exchange": ex, "instrument_id": iid}, timeout=10)
            if content:
                parsed = self._parse_content(content)
                result[iid]["tick"] = parsed if isinstance(parsed, dict) else {}

            # candles
            for style, count in candle_cfg:
                content = self.call_tool("fetch_candles", {
                    "exchange": ex, "instrument_id": iid,
                    "style": style, "count": -count, "simply": False
                }, timeout=10)
                candles = self._parse_candle_content(content)
                key = f"candles_{self._STYLE_KEY_MAP[style]}"
                result[iid][key] = _dedup_candles(candles)

        return result

    def fetch_ticks_only(self, instruments):
        """只获取tick数据(快速，用于高频刷新)"""
        if not instruments or not self._connected:
            return {}

        result = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            ex = inst["exchange"]
            content = self.call_tool("fetch_tick", {"exchange": ex, "instrument_id": iid}, timeout=8)
            if content:
                parsed = self._parse_content(content)
                result[iid] = {"tick": parsed if isinstance(parsed, dict) else {}}
        return result

    def fetch_candles_only(self, instruments, full=False):
        """只获取K线数据(顺序调用，用于后台刷新)"""
        if not instruments or not self._connected:
            return {}

        if full:
            candle_cfg = [("M1", 120), ("M5", 60), ("M15", 48), ("D1", 30)]
        else:
            candle_cfg = [("M1", 20), ("M5", 10), ("M15", 8), ("D1", 5)]

        result = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            ex = inst["exchange"]
            result[iid] = {}
            for style, count in candle_cfg:
                content = self.call_tool("fetch_candles", {
                    "exchange": ex, "instrument_id": iid,
                    "style": style, "count": -count, "simply": False
                }, timeout=10)
                candles = self._parse_candle_content(content)
                key = f"candles_{self._STYLE_KEY_MAP[style]}"
                result[iid][key] = _dedup_candles(candles)
        return result

    def _parse_content(self, content):
        """解析MCP content为Python对象"""
        if isinstance(content, list):
            texts = [c.get("text", "") for c in content if "text" in c]
            if len(texts) == 1:
                try:
                    return json.loads(texts[0])
                except:
                    return texts[0]
            return [json.loads(t) if t.startswith("{") or t.startswith("[") else t for t in texts]
        return content

    def _parse_candle_content(self, content):
        """解析K线content并标准化时间字段"""
        parsed = self._parse_content(content)
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and "datetime" in item and "time" not in item:
                    item["time"] = item.pop("datetime")
            return parsed
        return None

    def ensure_subscribed(self, instruments):
        """确保所有合约已订阅行情推送"""
        need_sub = []
        for inst in instruments:
            iid = inst["instrument_id"]
            if iid not in self._subscribed:
                need_sub.append(inst)

        if not need_sub:
            return True

        for inst in need_sub:
            rid = self._next_id()
            self._post("tools/call", {
                "name": "start_quote_feed",
                "arguments": {"exchange": inst["exchange"], "instrument_id": inst["instrument_id"]}
            }, rid)
            time.sleep(0.03)
            self._subscribed.add(inst["instrument_id"])

        time.sleep(0.8)
        return True

    def close(self):
        """关闭连接"""
        self._connected = False
        self._subscribed.clear()
        if self._resp:
            try:
                self._resp.close()
            except:
                pass

# 全局MCP客户端实例
mcp_client = MCPClient()

# ============================================================================
# 全局状态
# ============================================================================

class AppState:
    def __init__(self):
        self.instruments = []  # [{"instrument_id": "rb2510", "exchange": "SHFE", "label": "螺纹2510"}]
        self.data = {}  # {instrument_id: {tick, candles_1m, candles_5m, candles_1d, ...}}
        self.capital_info = None
        self.holdings_info = None
        self.positions = []  # MCP持仓
        self.connected = False
        self.start_time = None
        self.auto_trading = False
        self.signal_threshold = 20
        self.reversal_threshold = 15
        self.sl_pct = 1.0
        self.tp_pct = 2.5
        self.trade_volume = 1
        self.timeframe = "1m"
        self.ws_clients = set()
        self._tq_api = None
        self._tq_lock = threading.Lock()
        self.trade_log = []
        self.rankings_cache = {}
        self.instrument_configs = {}  # {instrument_id: {threshold, reversal, sl_pct, tp_pct, trade_volume}}
        self.tq_connected = False
        self.tq_error = ""

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                self.instruments = saved.get("instruments", [])
                self.auto_trading = saved.get("auto_trading", False)
                self.signal_threshold = saved.get("signal_threshold", 50)
                self.reversal_threshold = saved.get("reversal_threshold", 30)
                self.sl_pct = saved.get("sl_pct", 2.0)
                self.tp_pct = saved.get("tp_pct", 4.0)
                self.trade_volume = saved.get("trade_volume", 1)
                self.timeframe = saved.get("timeframe", "1m")
                self.instrument_configs = saved.get("instrument_configs", {})
                self.tq_user = saved.get("tq_user")
                self.tq_pass = saved.get("tq_pass")
            except:
                pass

    def save_state(self):
        try:
            data = {
                "instruments": self.instruments,
                "auto_trading": self.auto_trading,
                "signal_threshold": self.signal_threshold,
                "reversal_threshold": self.reversal_threshold,
                "sl_pct": self.sl_pct,
                "tp_pct": self.tp_pct,
                "trade_volume": self.trade_volume,
                "timeframe": self.timeframe,
                "instrument_configs": self.instrument_configs,
            }
            if hasattr(self, 'tq_user') and self.tq_user:
                data["tq_user"] = self.tq_user
            if hasattr(self, 'tq_pass') and self.tq_pass:
                data["tq_pass"] = self.tq_pass
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except:
            pass

state = AppState()
state.load_state()

def get_inst_config(iid):
    """获取品种独立配置，未设置则用全局默认"""
    ic = state.instrument_configs.get(iid, {})
    return {
        "threshold": ic.get("threshold", state.signal_threshold),
        "reversal": ic.get("reversal", state.reversal_threshold),
        "sl_pct": ic.get("sl_pct", state.sl_pct),
        "tp_pct": ic.get("tp_pct", state.tp_pct),
        "trade_volume": ic.get("trade_volume", state.trade_volume),
    }

# ============================================================================
# 交易时间判断
# ============================================================================

def is_trading_hours(now=None):
    if now is None:
        now = datetime.now()
    weekday = now.weekday()
    hour = now.hour
    minute = now.minute
    t = hour * 60 + minute
    if weekday >= 5:
        return False
    if 540 <= t <= 615:
        return True
    if 630 <= t <= 690:
        return True
    if 810 <= t <= 900:
        return True
    if t >= 1260:
        if weekday == 4:
            return False
        return True
    if t <= 150:
        if weekday == 5:
            return False
        return True
    return False

def get_next_open_time(now=None):
    if now is None:
        now = datetime.now()
    weekday = now.weekday()
    hour = now.hour
    minute = now.minute
    t = hour * 60 + minute
    sessions = [(540, "9:00"), (630, "10:30"), (810, "13:30"), (1260, "21:00")]
    for st_min, sname in sessions:
        if t < st_min:
            if st_min == 1260 and weekday == 4:
                continue
            return f"今日 {sname}"
    if weekday < 4:
        return f"明日 9:00"
    elif weekday == 5:
        return f"下周一 9:00"
    else:
        return f"下周一 9:00"

# ============================================================================
# 天勤量化数据源
# ============================================================================

def _to_tq_symbol(instrument_id, exchange):
    return f"{exchange}.{instrument_id}"

_tq_api_ref = None
_tq_lock = threading.Lock()
_trade_cooldown = {}  # {(instrument_id, direction): last_trade_timestamp}
_pos_refresh_ok = False  # 持仓刷新是否成功（默认False，必须先确认持仓才能交易）

def _get_tq_api():
    """获取已有的天勤API连接（不自动创建新连接）"""
    global _tq_api_ref
    with _tq_lock:
        if _tq_api_ref is not None:
            state.tq_connected = True
            state.tq_error = ""
            return _tq_api_ref, None
        state.tq_connected = False
        return None, "天勤未连接"

def _create_tq_api():
    """创建新的天勤API连接（仅在手动连接时调用）"""
    global _tq_api_ref
    with _tq_lock:
        # 先关闭旧连接
        if _tq_api_ref is not None:
            try:
                _tq_api_ref.close()
            except:
                pass
            _tq_api_ref = None
        try:
            from tqsdk import TqApi, TqAuth
            tq_user = getattr(state, 'tq_user', None)
            tq_pass = getattr(state, 'tq_pass', None)
            if not tq_user or not tq_pass:
                state.tq_connected = False
                state.tq_error = "未配置天勤账号"
                return None, "未配置天勤账号"
            api = TqApi(auth=TqAuth(tq_user, tq_pass))
            _tq_api_ref = api
            state.tq_connected = True
            state.tq_error = ""
            return api, None
        except Exception as e:
            state.tq_connected = False
            tb = traceback.format_exc()
            state.tq_error = str(e)
            print(f"[TQ] Connection error: {e}")
            print(f"[TQ] Traceback:\n{tb}")
            return None, str(e)

def connect_tq():
    """手动连接天勤，返回状态（连接后自动拉取一次数据）"""
    state.tq_connected = False
    # 创建新连接
    api, err = _create_tq_api()
    if not api:
        return {"ok": False, "error": err}
    # 连接成功，立即拉取一次数据
    if state.instruments:
        data = fetch_data_from_tq(state.instruments)
        if data:
            state.data = data
            return {"ok": True, "status": f"已连接，{len(data)}个品种数据就绪"}
        else:
            return {"ok": True, "status": "已连接（数据拉取中...）"}
    return {"ok": True, "status": "已连接"}

def disconnect_tq():
    """断开天勤连接"""
    global _tq_api_ref
    with _tq_lock:
        if _tq_api_ref is not None:
            try:
                _tq_api_ref.close()
            except:
                pass
            _tq_api_ref = None
    state.tq_connected = False
    state.tq_error = ""
    return {"ok": True, "status": "已断开"}

def _dedup_candles(candles):
    """去重K线：按time字段去重，保留最后一条（最新的）"""
    if not candles or not isinstance(candles, list):
        return candles
    seen = {}
    for c in candles:
        t = c.get("time", "")
        if t:
            seen[t] = c
    # 按time排序返回
    return list(seen.values())

def fetch_data_from_tq(instruments):
    api, err = _get_tq_api()
    if err:
        print(f"[TQ] fetch_data_from_tq: no API - {err}")
        return None
    try:
        print(f"[TQ] Subscribing {len(instruments)} instruments...")
        subs = {}
        for inst in instruments:
            sym = _to_tq_symbol(inst["instrument_id"], inst["exchange"])
            subs[inst["instrument_id"]] = {
                "tick": api.get_quote(sym),
                "k1m": api.get_kline_serial(sym, 60, data_length=120),
                "k5m": api.get_kline_serial(sym, 300, data_length=60),
                "k15m": api.get_kline_serial(sym, 900, data_length=48),
                "k1d": api.get_kline_serial(sym, 86400, data_length=30),
            }
        print(f"[TQ] Waiting for data update (20s)...")
        api.wait_update(deadline=time.time() + 20)
        print(f"[TQ] Data update received, building result...")
        result = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            sub = subs[iid]
            tick = sub["tick"]
            def _safe_f(v):
                try:
                    f = float(v)
                    return f if f == f else 0
                except:
                    return 0
            def _safe_i(v):
                try:
                    i = int(float(v))
                    return i if i == i else 0
                except:
                    return 0
            tick_data = {
                "last_price": _safe_f(tick.last_price),
                "open": _safe_f(tick.open),
                "high": _safe_f(tick.highest),
                "low": _safe_f(tick.lowest),
                "pre_close": _safe_f(tick.pre_close),
                "volume": _safe_i(tick.volume),
                "upper_limit": _safe_f(tick.upper_limit),
                "lower_limit": _safe_f(tick.lower_limit),
            }
            # 5档买卖盘
            for lvl in range(1, 6):
                tick_data[f"ask_price{lvl}"] = _safe_f(getattr(tick, f"ask_price{lvl}", 0))
                tick_data[f"ask_volume{lvl}"] = _safe_i(getattr(tick, f"ask_volume{lvl}", 0))
                tick_data[f"bid_price{lvl}"] = _safe_f(getattr(tick, f"bid_price{lvl}", 0))
                tick_data[f"bid_volume{lvl}"] = _safe_i(getattr(tick, f"bid_volume{lvl}", 0))
            def _klines_to_list(kdf):
                out = []
                for _, row in kdf.iterrows():
                    import math
                    o = float(row.get("open", 0))
                    h = float(row.get("high", 0))
                    l = float(row.get("low", 0))
                    c = float(row.get("close", 0))
                    v = float(row.get("volume", 0))
                    if any(math.isnan(x) for x in [o, h, l, c]):
                        continue
                    if o <= 0 or v <= 0:
                        continue
                    t_raw = row.get("datetime", 0)
                    if isinstance(t_raw, (int, float)) and not math.isnan(t_raw) and t_raw > 0:
                        ts = t_raw / 1e9 if t_raw > 1e15 else t_raw
                        t_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
                    else:
                        t_str = str(t_raw)
                    out.append({"time": t_str, "open": o, "high": h, "low": l, "close": c, "volume": int(v)})
                return out
            result[iid] = {
                "tick": tick_data,
                "candles_1m": _dedup_candles(_klines_to_list(sub["k1m"])),
                "candles_5m": _dedup_candles(_klines_to_list(sub["k5m"])),
                "candles_15m": _dedup_candles(_klines_to_list(sub["k15m"])),
                "candles_1d": _dedup_candles(_klines_to_list(sub["k1d"])),
            }
            print(f"[TQ] {iid}: 1m={len(result[iid]['candles_1m'])}, 5m={len(result[iid]['candles_5m'])}, 15m={len(result[iid]['candles_15m'])}, 1d={len(result[iid]['candles_1d'])}")
        # Save to K-line cache
        for iid, rd in result.items():
            for tf_key in ["candles_1m", "candles_5m", "candles_15m", "candles_1d"]:
                tf = tf_key.replace("candles_", "")
                if rd.get(tf_key):
                    save_kline_cache(iid, tf, rd[tf_key])
        return result
    except Exception as e:
        print(f"TQ fetch error: {e}")
        return None

# ============================================================================
# TQ 实时数据流 (主数据源)
# ============================================================================

class TQFeed:
    """天勤实时数据流 - 订阅一次，持续推送更新"""
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def __init__(self):
        self.api = None
        self.subs = {}  # {instrument_id: {tick, k1m, k5m, k15m, k1d}}
        self._thread = None
        self._running = False
        self._connected = False
        self._error = ""
        self._data_lock = threading.Lock()
        self._cycle = 0

    def connect(self, instruments):
        """连接TQ并订阅所有品种（在调用线程执行）"""
        global _tq_api_ref
        # 清理旧连接
        if self.api is not None:
            try:
                self.api.close()
            except:
                pass
            self.api = None
        _tq_api_ref = None
        self.subs = {}
        self._connected = False

        try:
            from tqsdk import TqApi, TqAuth
            tq_user = getattr(state, 'tq_user', None)
            tq_pass = getattr(state, 'tq_pass', None)
            if not tq_user or not tq_pass:
                self._error = "未配置天勤账号"
                state.tq_connected = False
                state.tq_error = self._error
                return False

            _dlog(f"[TQFeed] Connecting TQ API...")
            self.api = TqApi(auth=TqAuth(tq_user, tq_pass))
            _tq_api_ref = self.api
            _dlog(f"[TQFeed] TQ connected, subscribing {len(instruments)} instruments...")

            # 订阅所有品种
            self._subscribe(instruments)
            self._connected = True
            self._error = ""
            state.tq_connected = True
            state.tq_error = ""
            _dlog(f"[TQFeed] Subscribed OK")
            return True
        except Exception as e:
            self._error = str(e)
            self._connected = False
            state.tq_connected = False
            state.tq_error = self._error
            _dlog(f"[TQFeed] Connect error: {e}")
            tb = traceback.format_exc()
            _dlog(f"[TQFeed] Traceback:\n{tb}")
            return False

    def _subscribe(self, instruments):
        """订阅品种行情和K线（只在初始化时调用一次）"""
        self.subs = {}
        for inst in instruments:
            sym = _to_tq_symbol(inst["instrument_id"], inst["exchange"])
            self.subs[inst["instrument_id"]] = {
                "tick": self.api.get_quote(sym),
                "k1m": self.api.get_kline_serial(sym, 60, data_length=120),
                "k5m": self.api.get_kline_serial(sym, 300, data_length=60),
                "k15m": self.api.get_kline_serial(sym, 900, data_length=48),
                "k1d": self.api.get_kline_serial(sym, 86400, data_length=30),
            }
        _dlog(f"[TQFeed] Subscriptions created for {len(self.subs)} instruments")

    def _safe_f(self, v):
        try:
            f = float(v)
            return f if f == f else 0
        except:
            return 0

    def _safe_i(self, v):
        try:
            i = int(float(v))
            return i if i == i else 0
        except:
            return 0

    def _build_tick(self, quote):
        """从quote对象构建tick字典"""
        tick = {
            "last_price": self._safe_f(quote.last_price),
            "open": self._safe_f(quote.open),
            "high": self._safe_f(quote.highest),
            "low": self._safe_f(quote.lowest),
            "pre_close": self._safe_f(quote.pre_close),
            "volume": self._safe_i(quote.volume),
            "upper_limit": self._safe_f(quote.upper_limit),
            "lower_limit": self._safe_f(quote.lower_limit),
        }
        for lvl in range(1, 6):
            tick[f"ask_price{lvl}"] = self._safe_f(getattr(quote, f"ask_price{lvl}", 0))
            tick[f"ask_volume{lvl}"] = self._safe_i(getattr(quote, f"ask_volume{lvl}", 0))
            tick[f"bid_price{lvl}"] = self._safe_f(getattr(quote, f"bid_price{lvl}", 0))
            tick[f"bid_volume{lvl}"] = self._safe_i(getattr(quote, f"bid_volume{lvl}", 0))
        return tick

    def _klines_to_list(self, kdf):
        """K线DataFrame转list[dict]"""
        import math
        out = []
        for _, row in kdf.iterrows():
            ts = row.get("datetime", "")
            if not ts or (isinstance(ts, float) and math.isnan(ts)):
                continue
            t = str(ts)
            out.append({
                "time": t,
                "open": self._safe_f(row.get("open", 0)),
                "high": self._safe_f(row.get("high", 0)),
                "low": self._safe_f(row.get("low", 0)),
                "close": self._safe_f(row.get("close", 0)),
                "volume": self._safe_i(row.get("volume", 0)),
            })
        return out

    def _build_result(self):
        """从当前订阅数据构建结果字典"""
        result = {}
        for iid, sub in self.subs.items():
            tick = sub["tick"]
            entry = {"tick": self._build_tick(tick)}
            for key, kdf in [("candles_1m", sub["k1m"]), ("candles_5m", sub["k5m"]),
                             ("candles_15m", sub["k15m"]), ("candles_1d", sub["k1d"])]:
                entry[key] = self._klines_to_list(kdf)
            result[iid] = entry
        return result

    def fetch_update(self, timeout=3):
        """等待数据更新并返回最新数据（阻塞调用，在TQ线程中运行）"""
        if not self.api or not self.subs:
            return None
        try:
            changed = self.api.wait_update(deadline=time.time() + timeout)
            if not changed:
                return None
            # 检查是否有行情变化
            any_quote_changed = False
            for iid, sub in self.subs.items():
                if self.api.is_changing(sub["tick"]):
                    any_quote_changed = True
                    break
            if not any_quote_changed:
                return None
            return self._build_result()
        except Exception as e:
            _dlog(f"[TQFeed] fetch_update error: {e}")
            self._connected = False
            state.tq_connected = False
            state.tq_error = str(e)
            return None

    def stream_loop(self, instruments):
        """持续流式推送（在专用线程中运行）"""
        _dlog(f"[TQFeed] stream_loop started for {len(instruments)} instruments")
        while self._running:
            if not self._connected:
                _dlog(f"[TQFeed] Not connected, attempting reconnect...")
                time.sleep(5)
                self.connect(instruments)
                continue

            data = self.fetch_update(timeout=3)
            if data:
                with self._data_lock:
                    state.data = _merge_data(state.data, data)
                self._cycle += 1
                if self._cycle <= 5 or self._cycle % 50 == 0:
                    _dlog(f"[TQFeed] cycle {self._cycle}: updated {len(data)} instruments")

            # 交易时段短间隔，非交易时段稍长
            time.sleep(0.5 if is_trading_hours() else 3)

    def start_streaming(self, instruments):
        """启动后台流式线程"""
        if self._thread and self._thread.is_alive():
            _dlog(f"[TQFeed] Already streaming")
            return
        self._running = True
        self._thread = threading.Thread(target=self.stream_loop, args=(instruments,), daemon=True)
        self._thread.start()
        _dlog(f"[TQFeed] Streaming thread started")

    def stop(self):
        """停止流式推送"""
        self._running = False
        global _tq_api_ref
        if self.api is not None:
            try:
                self.api.close()
            except:
                pass
            self.api = None
        _tq_api_ref = None
        self._connected = False
        state.tq_connected = False
        _dlog(f"[TQFeed] Stopped")

# ============================================================================
# MCP 连接
# ============================================================================

def one_shot_fetch(instruments):
    if not instruments:
        return {}
    try:
        resp = requests.get(MCP_SSE_URL, stream=True, timeout=40, headers={"Accept": "text/event-stream"})
        ep_state = {"ep": None}
        responses = {}
        lock = threading.Lock()

        def listen():
            et = "message"
            dl = []
            for line in resp.iter_lines(decode_unicode=True):
                if line.startswith("event:"):
                    et = line[6:].strip()
                elif line.startswith("data:"):
                    dl.append(line[5:].strip())
                elif line == "":
                    if dl:
                        raw = "\n".join(dl)
                        if et == "endpoint":
                            ep_state["ep"] = raw
                        elif et == "message":
                            try:
                                d = json.loads(raw)
                                if "id" in d:
                                    with lock:
                                        responses[d["id"]] = d
                            except:
                                pass
                        et = "message"
                        dl = []

        t = threading.Thread(target=listen, daemon=True)
        t.start()

        for _ in range(50):
            if ep_state["ep"]:
                break
            time.sleep(0.1)

        if not ep_state["ep"]:
            resp.close()
            return {}

        u = f"http://127.0.0.1:3000{ep_state['ep']}" if ep_state["ep"].startswith("/") else ep_state["ep"]

        # handshake
        requests.post(u, json={
            "jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                       "clientInfo": {"name": "T", "version": "1.0"}}
        }, timeout=5)
        for _ in range(50):
            with lock:
                if 0 in responses:
                    responses.pop(0)
                    break
            time.sleep(0.05)
        requests.post(u, json={"jsonrpc": "2.0", "method": "notifications/initialized"}, timeout=5)

        result = {}
        for idx, inst in enumerate(instruments):
            rid = idx + 1
            iid = inst["instrument_id"]
            ex = inst["exchange"]

            # subscribe
            requests.post(u, json={
                "jsonrpc": "2.0", "id": 100 + idx, "method": "tools/call",
                "params": {"name": "start_quote_feed",
                           "arguments": {"exchange": ex, "instrument_id": iid}}
            }, timeout=5)
            time.sleep(0.05)

        time.sleep(1.0)

        # fetch data for each instrument
        for idx, inst in enumerate(instruments):
            rid = idx + 1
            iid = inst["instrument_id"]
            ex = inst["exchange"]

            # tick
            requests.post(u, json={
                "jsonrpc": "2.0", "id": 200 + idx, "method": "tools/call",
                "params": {"name": "fetch_tick", "arguments": {"exchange": ex, "instrument_id": iid}}
            }, timeout=5)
            time.sleep(0.05)

            # candles
            for style, count, rid_off in [("M1", 120, 300), ("M5", 60, 400), ("M15", 48, 600), ("D1", 30, 500)]:
                requests.post(u, json={
                    "jsonrpc": "2.0", "id": rid_off + idx, "method": "tools/call",
                    "params": {"name": "fetch_candles",
                               "arguments": {"exchange": ex, "instrument_id": iid,
                                             "style": style, "count": -count, "simply": False}}
                }, timeout=5)
                time.sleep(0.05)

        # collect responses (up to 15s)
        deadline = time.time() + 15
        expected_ids = set()
        for idx in range(len(instruments)):
            expected_ids.add(200 + idx)
            expected_ids.add(300 + idx)
            expected_ids.add(400 + idx)
            expected_ids.add(500 + idx)
            expected_ids.add(600 + idx)

        while time.time() < deadline:
            with lock:
                if expected_ids.issubset(set(responses.keys())):
                    break
            time.sleep(0.1)

        def parse(content):
            if isinstance(content, list):
                texts = [c.get("text", "") for c in content if "text" in c]
                if len(texts) == 1:
                    try:
                        return json.loads(texts[0])
                    except:
                        return texts[0]
                return [json.loads(t) if t.startswith("{") or t.startswith("[") else t for t in texts]
            return content

        def normalize_candles(data):
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "datetime" in item and "time" not in item:
                        item["time"] = item.pop("datetime")
            return data

        for idx, inst in enumerate(instruments):
            iid = inst["instrument_id"]
            tick = parse(responses.get(200 + idx, {}).get("result", {}).get("content", []))
            candles_1m = parse(responses.get(300 + idx, {}).get("result", {}).get("content", []))
            candles_5m = parse(responses.get(400 + idx, {}).get("result", {}).get("content", []))
            candles_1d = parse(responses.get(500 + idx, {}).get("result", {}).get("content", []))
            candles_15m = parse(responses.get(600 + idx, {}).get("result", {}).get("content", []))

            result[iid] = {
                "tick": tick if isinstance(tick, dict) else {},
                "candles_1m": _dedup_candles(normalize_candles(candles_1m if isinstance(candles_1m, list) else None)),
                "candles_5m": _dedup_candles(normalize_candles(candles_5m if isinstance(candles_5m, list) else None)),
                "candles_15m": _dedup_candles(normalize_candles(candles_15m if isinstance(candles_15m, list) else None)),
                "candles_1d": _dedup_candles(normalize_candles(candles_1d if isinstance(candles_1d, list) else None)),
            }

        resp.close()
        return result
    except Exception as e:
        print(f"MCP fetch error: {e}")
        return {}

# ============================================================================
# MCP 交易执行
# ============================================================================

def _mcp_parse(content):
    if isinstance(content, list):
        # 先尝试标准text格式
        texts = [c.get("text", "") for c in content if "text" in c]
        if len(texts) == 1:
            try: return json.loads(texts[0])
            except: return texts[0]
        if texts:
            return texts
        # 非标准格式：直接返回原始dict列表（如K线数据）
        dicts = [c for c in content if isinstance(c, dict) and c]
        if dicts:
            return dicts
    return content

def _mcp_trade_call(tool_name, arguments):
    """通过持久化MCP连接执行交易/数据调用"""
    return mcp_client.call_tool_parsed(tool_name, arguments, timeout=30)

def _add_trade_log(event_type, instrument_id, direction="", volume=0, price=0, reason="", extra=""):
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "type": event_type,
        "instrument_id": instrument_id,
        "direction": direction,
        "volume": volume,
        "price": price,
        "reason": reason,
        "extra": extra,
    }
    state.trade_log.insert(0, entry)
    if len(state.trade_log) > 200:
        state.trade_log = state.trade_log[:200]

def submit_trade(instrument_id, exchange, direction, volume, price=0):
    """开仓下单"""
    result = _mcp_trade_call("submit_open", {
        "exchange": exchange,
        "instrument_id": instrument_id,
        "direction": direction,
        "volume": volume,
        "price": price,
    })
    _add_trade_log("开仓", instrument_id, direction, volume,
                   reason="手动下单" if not state.auto_trading else "自动交易")
    return result

def close_position(instrument_id, exchange, direction, volume, reason="手动平仓"):
    """平仓"""
    result = _mcp_trade_call("submit_close", {
        "exchange": exchange,
        "instrument_id": instrument_id,
        "direction": direction,
        "volume": volume,
    })
    _add_trade_log("平仓", instrument_id, direction, volume, reason=reason)
    return result

def set_stop_loss(instrument_id, exchange, direction, price):
    """设置止损"""
    result = _mcp_trade_call("set_stop_loss", {
        "exchange": exchange,
        "instrument_id": instrument_id,
        "direction": direction,
        "price": price,
    })
    _add_trade_log("止损设置", instrument_id, direction=direction, price=price)
    return result

def set_take_profit(instrument_id, exchange, direction, price):
    """设置止盈"""
    result = _mcp_trade_call("set_take_profit", {
        "exchange": exchange,
        "instrument_id": instrument_id,
        "direction": direction,
        "price": price,
    })
    _add_trade_log("止盈设置", instrument_id, direction=direction, price=price)
    return result

# ============================================================================
# 自动交易引擎
# ============================================================================

def execute_auto_trades():
    """检查信号并自动执行交易 — v3多周期确认+ATR动态止损版"""
    global _trade_cooldown
    if not state.auto_trading or not state.data or not state.instruments:
        return

    now_ts = time.time()
    cooldown_sec = 180  # 同品种开仓冷却3分钟

    for inst in state.instruments:
        iid = inst["instrument_id"]
        ex = inst.get("exchange", "")
        d = state.data.get(iid, {})
        tick = d.get("tick", {})
        if not isinstance(tick, dict) or not tick:
            continue

        analysis = analyze_instrument(state.data, inst, state.timeframe)
        if not analysis:
            continue

        score = analysis.get("total_score", 0)
        cp = tick.get("last_price", 0)
        if cp <= 0:
            continue

        atr = analysis.get("atr", 0)
        atr_pct = analysis.get("atr_pct", 0)
        htf_trend = analysis.get("htf_trend", {})
        htf_dir = htf_trend.get("direction", 0)
        htf_str = htf_trend.get("strength", 0)

        ic = get_inst_config(iid)
        threshold = ic["threshold"]
        reversal = ic["reversal"]

        # ---- 查询实际持仓 ----
        long_vol = 0
        short_vol = 0
        query_ok = False
        try:
            hr = _mcp_trade_call("fetch_holding", {"instrument_id": iid})
            if hr.get("success"):
                query_ok = True
                hd = hr.get("result", {})
                if isinstance(hd, dict):
                    long_info = hd.get("long") or {}
                    short_info = hd.get("short") or {}
                    if isinstance(long_info, dict):
                        long_vol = long_info.get("volume_today", 0) + long_info.get("volume_his", 0)
                    if isinstance(short_info, dict):
                        short_vol = short_info.get("volume_today", 0) + short_info.get("volume_his", 0)
            else:
                print(f"[AutoTrade] {iid} fetch_holding failed: {hr.get('error')}")
                continue
        except Exception as e:
            print(f"[AutoTrade] {iid} holding check error: {e}")
            continue

        if not query_ok:
            continue

        total_vol = long_vol + short_vol

        # ---- 第一步：平仓（两边都检查，不用elif） ----
        did_close = False

        if short_vol > 0 and score >= reversal:
            _add_trade_log("反转", iid, "sell", reason=f"评分{score:.0f}>=反转{reversal},平空{short_vol}手")
            close_position(iid, ex, "sell", short_vol, reason="反转平空")
            did_close = True
            print(f"[AutoTrade] {iid} 平空 {short_vol}手 (score={score:.0f})")

        if long_vol > 0 and score <= -reversal:
            _add_trade_log("反转", iid, "buy", reason=f"评分{score:.0f}<=-{reversal},平多{long_vol}手")
            close_position(iid, ex, "buy", long_vol, reason="反转平多")
            did_close = True
            print(f"[AutoTrade] {iid} 平多 {long_vol}手 (score={score:.0f})")

        # ---- 第二步：本循环平过仓就不开新仓 ----
        if did_close:
            print(f"[AutoTrade] {iid} 已平仓，本轮不开新仓")
            continue

        # ---- 第三步：有持仓就不开新仓 ----
        if total_vol > 0:
            continue

        # ---- 第四步：开仓前过滤 ----
        cd_key = iid
        if now_ts - _trade_cooldown.get(cd_key, 0) < cooldown_sec:
            continue

        # 过滤1: 波动率过滤 — ATR%太低(死水)或太高(恐慌)都不交易
        if atr > 0 and cp > 0:
            if atr_pct < 0.03:
                print(f"[AutoTrade] {iid} 跳过: 波动率过低 ATR%={atr_pct:.3f}%")
                continue
            if atr_pct > 3.0:
                print(f"[AutoTrade] {iid} 跳过: 波动率过高 ATR%={atr_pct:.3f}%")
                continue

        # 过滤2: 多周期确认 — 逆强趋势不开仓
        if score > 0 and htf_dir == -1 and htf_str >= 60:
            print(f"[AutoTrade] {iid} 跳过: 逆高周期空头(强度{htf_str:.0f})做多")
            continue
        if score < 0 and htf_dir == 1 and htf_str >= 60:
            print(f"[AutoTrade] {iid} 跳过: 逆高周期多头(强度{htf_str:.0f})做空")
            continue

        # 过滤3: Delta方向确认 — 最近3根K线至少2根Delta与信号同向
        ds = analysis.get("delta", [])
        if len(ds) >= 3:
            recent_deltas = [dd["delta"] for dd in ds[-3:]]
            if score > 0:
                pos_count = sum(1 for d in recent_deltas if d > 0)
                if pos_count < 2:
                    print(f"[AutoTrade] {iid} 跳过: Delta确认不足(多{pos_count}/3)")
                    continue
            elif score < 0:
                neg_count = sum(1 for d in recent_deltas if d < 0)
                if neg_count < 2:
                    print(f"[AutoTrade] {iid} 跳过: Delta确认不足(空{neg_count}/3)")
                    continue

        # ---- 第五步：ATR动态止损开仓 ----
        # ATR止损倍数: 2倍ATR，最低不低于固定百分比止损
        atr_sl_multiplier = 2.0
        fixed_sl_pct = ic["sl_pct"] / 100

        if score >= threshold:
            # ATR动态止损
            atr_sl_dist = atr * atr_sl_multiplier if atr > 0 else cp * fixed_sl_pct
            fixed_sl_dist = cp * fixed_sl_pct
            sl_dist = max(atr_sl_dist, fixed_sl_dist)  # 取较大值，保安全
            sl = round(cp - sl_dist, 2)
            # ATR动态止盈: 至少3倍ATR或固定止盈
            atr_tp_dist = atr * 3.0 if atr > 0 else cp * ic["tp_pct"] / 100
            fixed_tp_dist = cp * ic["tp_pct"] / 100
            tp_dist = max(atr_tp_dist, fixed_tp_dist)
            tp = round(cp + tp_dist, 2)

            _add_trade_log("信号", iid, "buy", reason=f"评分{score:.0f}>=阈值{threshold},ATR={atr:.1f},开多")
            result = submit_trade(iid, ex, "buy", ic["trade_volume"], cp)
            if result.get("success"):
                _trade_cooldown[cd_key] = now_ts
                set_stop_loss(iid, ex, "buy", sl)
                time.sleep(0.5)
                set_take_profit(iid, ex, "buy", tp)
                print(f"[AutoTrade] {iid} 开多 @ {cp}, SL={sl}({sl_dist/cp*100:.2f}%), TP={tp}({tp_dist/cp*100:.2f}%)")

        elif score <= -threshold:
            atr_sl_dist = atr * atr_sl_multiplier if atr > 0 else cp * fixed_sl_pct
            fixed_sl_dist = cp * fixed_sl_pct
            sl_dist = max(atr_sl_dist, fixed_sl_dist)
            sl = round(cp + sl_dist, 2)
            atr_tp_dist = atr * 3.0 if atr > 0 else cp * ic["tp_pct"] / 100
            fixed_tp_dist = cp * ic["tp_pct"] / 100
            tp_dist = max(atr_tp_dist, fixed_tp_dist)
            tp = round(cp - tp_dist, 2)

            _add_trade_log("信号", iid, "sell", reason=f"评分{score:.0f}<=-{threshold},ATR={atr:.1f},开空")
            result = submit_trade(iid, ex, "sell", ic["trade_volume"], cp)
            if result.get("success"):
                _trade_cooldown[cd_key] = now_ts
                set_stop_loss(iid, ex, "sell", sl)
                time.sleep(0.5)
                set_take_profit(iid, ex, "sell", tp)
                print(f"[AutoTrade] {iid} 开空 @ {cp}, SL={sl}({sl_dist/cp*100:.2f}%), TP={tp}({tp_dist/cp*100:.2f}%)")

def _refresh_positions():
    """从MCP刷新持仓数据（同步）"""
    global _pos_refresh_ok
    try:
        result = _mcp_trade_call("fetch_all_holdings", {"simple": True})
        if not result.get("success"):
            _pos_refresh_ok = False
            return
        raw = result.get("result", {})
        positions = []
        if isinstance(raw, dict):
            for investor, contracts in raw.items():
                if not isinstance(contracts, dict):
                    continue
                for iid, hedgeflags in contracts.items():
                    if not isinstance(hedgeflags, dict):
                        continue
                    for hf, pos_raw in hedgeflags.items():
                        if isinstance(pos_raw, str):
                            try:
                                pos_raw = eval(pos_raw)
                            except:
                                continue
                        if not isinstance(pos_raw, dict):
                            continue
                        for side in ["long", "short"]:
                            info = pos_raw.get(side, {})
                            vol = info.get("position", 0)
                            if vol > 0:
                                positions.append({
                                    "instrument_id": iid,
                                    "direction": side,
                                    "volume": vol,
                                })
        state.positions = positions
        _pos_refresh_ok = True
    except Exception as e:
        _pos_refresh_ok = False
        print(f"Position refresh error: {e}")

async def auto_trade_loop():
    """后台自动交易循环"""
    await asyncio.sleep(5)
    while True:
        if state.auto_trading and is_trading_hours():
            try:
                # 先刷新持仓再执行交易
                await asyncio.to_thread(_refresh_positions)
                await asyncio.to_thread(execute_auto_trades)
            except Exception as e:
                print(f"Auto trade error: {e}")
        await asyncio.sleep(15)

# ============================================================================
# 机构持仓排名 (akshare)
# ============================================================================

def fetch_rankings_sync(exchange=None):
    """获取机构持仓排名, 返回缓存"""
    now = datetime.now()
    cache_key = exchange or "all"
    cached = state.rankings_cache.get(cache_key)
    if cached and (now - cached["time"]).total_seconds() < 3600:
        return cached["data"]

    try:
        import akshare as ak
        results = {}
        date_str = (now - timedelta(days=1)).strftime("%Y%m%d")

        # 目标品种
        shfe_vars = ["RB", "AG", "FU"]
        czce_targets = {"FG": "FG609", "SA": "SA609", "MA": "MA609", "TA": "TA609", "RM": "RM609"}

        if not exchange or exchange == "SHFE":
            shfe_data = []
            try:
                shfe_result = ak.get_shfe_rank_table(date=date_str, vars_list=shfe_vars)
                if isinstance(shfe_result, dict):
                    # 只取当前关注的合约
                    target_contracts = ["rb2610", "ag2609", "fu2609"]
                    for contract, df in shfe_result.items():
                        if contract.lower() not in target_contracts:
                            continue
                        if not hasattr(df, 'iterrows'):
                            continue
                        for _, row in df.head(20).iterrows():
                            shfe_data.append({
                                "exchange": "SHFE",
                                "symbol": contract,
                                "rank": int(row.get("rank", 0)),
                                "member": str(row.get("vol_party_name", "")),
                                "volume": int(float(str(row.get("vol", 0)).replace(",", ""))),
                                "vol_chg": int(float(str(row.get("vol_chg", 0)).replace(",", ""))),
                                "long_pos": int(float(str(row.get("long_open_interest", 0)).replace(",", ""))),
                                "long_chg": int(float(str(row.get("long_open_interest_chg", 0)).replace(",", ""))),
                                "short_pos": int(float(str(row.get("short_open_interest", 0)).replace(",", ""))),
                                "short_chg": int(float(str(row.get("short_open_interest_chg", 0)).replace(",", ""))),
                            })
            except Exception as e:
                print(f"[Rankings] SHFE error: {e}")
            results["SHFE"] = shfe_data

        if not exchange or exchange == "CZCE":
            czce_data = []
            try:
                czce_result = ak.get_rank_table_czce(date=date_str)
                if isinstance(czce_result, dict):
                    for variety, contract in czce_targets.items():
                        df = czce_result.get(contract)
                        if df is None:
                            df = czce_result.get(variety)
                        if df is None or not hasattr(df, 'iterrows'):
                            continue
                        for _, row in df.head(20).iterrows():
                            czce_data.append({
                                "exchange": "CZCE",
                                "symbol": contract,
                                "rank": int(row.get("rank", 0)),
                                "member": str(row.get("vol_party_name", "")),
                                "volume": int(float(str(row.get("vol", 0)).replace(",", ""))),
                                "vol_chg": int(float(str(row.get("vol_chg", 0)).replace(",", ""))),
                                "long_pos": int(float(str(row.get("long_open_interest", 0)).replace(",", ""))),
                                "long_chg": int(float(str(row.get("long_open_interest_chg", 0)).replace(",", ""))),
                                "short_pos": int(float(str(row.get("short_open_interest", 0)).replace(",", ""))),
                                "short_chg": int(float(str(row.get("short_open_interest_chg", 0)).replace(",", ""))),
                            })
            except Exception as e:
                print(f"[Rankings] CZCE error: {e}")
            results["CZCE"] = czce_data

        state.rankings_cache[cache_key] = {"time": now, "data": results}
        return results
    except Exception as e:
        print(f"Rankings fetch error: {e}")
        if cached:
            return cached["data"]
        return {}

# ============================================================================
# 分析函数
# ============================================================================

def calc_delta(candles):
    """改进版Delta: 通达信资金流向公式 VOL/((H-L)*2-|C-O|)
    通过K线结构估算主买/主卖量，比简单60/40分配准确得多"""
    deltas = []
    for c in candles:
        o = c.get("open", 0)
        h = c.get("high", 0)
        l = c.get("low", 0)
        cl = c.get("close", 0)
        vol = c.get("volume", 0)
        t = c.get("time", "")
        if vol <= 0 or h <= l:
            continue

        rng = h - l
        body = abs(cl - o)
        denom = rng * 2 - body
        if denom <= 0:
            factor = vol / max(rng, 1)
        else:
            factor = vol / denom

        if cl >= o:
            # 通达信: 主买=(H-O)+(O-L)*body_ratio
            bv = factor * (h - o + (o - l) * (cl - o) / max(cl - l, 0.001))
            sv = vol - bv
        else:
            sv = factor * (h - o + (o - l) * (o - cl) / max(h - cl, 0.001))
            bv = vol - sv

        bv = max(0, min(vol, bv))
        sv = max(0, min(vol, sv))

        d = bv - sv
        deltas.append({"time": t, "delta": round(d, 1), "buy_vol": round(bv, 1),
                        "sell_vol": round(sv, 1), "volume": vol, "close": cl, "open": o})
    return deltas

def calc_atr(candles, period=14):
    """计算ATR(Average True Range)，用于动态止损和波动率过滤"""
    if not candles or len(candles) < period + 1:
        return 0
    true_ranges = []
    for i in range(1, len(candles)):
        h = candles[i].get("high", 0)
        l = candles[i].get("low", 0)
        pc = candles[i-1].get("close", 0)
        if h <= 0 or l <= 0 or pc <= 0:
            continue
        tr = max(h - l, abs(h - pc), abs(l - pc))
        true_ranges.append(tr)
    if len(true_ranges) < period:
        return sum(true_ranges) / max(len(true_ranges), 1)
    # 使用EMA平滑的ATR
    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

def get_higher_tf_trend(data, inst, current_tf="1m"):
    """分析高周期趋势方向，用于多周期确认过滤
    返回: {\"direction\": 1/-1/0, \"strength\": 0-100, \"trend_label\": str}
    direction: 1=多头, -1=空头, 0=震荡
    """
    iid = inst["instrument_id"]
    d = data.get(iid, {})

    # 根据当前周期选择高周期
    if current_tf == "1m":
        htfs = ["candles_5m", "candles_15m"]
    elif current_tf == "5m":
        htfs = ["candles_15m", "candles_1d"]
    else:
        htfs = ["candles_1d"]

    scores = []
    for htf_key in htfs:
        candles = d.get(htf_key, [])
        if not candles or len(candles) < 10:
            continue

        # 用EMA交叉判断趋势: 短期EMA vs 长期EMA
        closes = [c.get("close", 0) for c in candles if c.get("close", 0) > 0]
        if len(closes) < 10:
            continue

        short_ema = closes[-1]
        long_ema = closes[-1]
        # 计算短期EMA(5)
        k_s = 2.0 / (min(5, len(closes)) + 1)
        ema_s = closes[0]
        for c in closes[1:]:
            ema_s = c * k_s + ema_s * (1 - k_s)
        short_ema = ema_s

        # 计算长期EMA(20)
        k_l = 2.0 / (min(20, len(closes)) + 1)
        ema_l = closes[0]
        for c in closes[1:]:
            ema_l = c * k_l + ema_l * (1 - k_l)
        long_ema = ema_l

        # 价格相对EMA位置
        last_close = closes[-1]
        ema_diff = (short_ema - long_ema) / max(long_ema, 1) * 100

        # 近期K线方向
        recent = candles[-5:]
        up_count = sum(1 for c in recent if c.get("close", 0) > c.get("open", 0))

        # 综合判断
        if ema_diff > 0.05 and up_count >= 3:
            scores.append(1)  # 强多头
        elif ema_diff > 0.02 and up_count >= 3:
            scores.append(0.5)  # 弱多头
        elif ema_diff < -0.05 and up_count <= 2:
            scores.append(-1)  # 强空头
        elif ema_diff < -0.02 and up_count <= 2:
            scores.append(-0.5)  # 弱空头
        else:
            scores.append(0)  # 震荡

    if not scores:
        return {"direction": 0, "strength": 0, "trend_label": "数据不足"}

    avg = sum(scores) / len(scores)
    strength = min(100, abs(avg) * 100)

    if avg >= 0.5:
        return {"direction": 1, "strength": round(strength, 1), "trend_label": "高周期多头"}
    elif avg <= -0.5:
        return {"direction": -1, "strength": round(strength, 1), "trend_label": "高周期空头"}
    elif avg > 0:
        return {"direction": 1, "strength": round(strength, 1), "trend_label": "高周期偏多"}
    elif avg < 0:
        return {"direction": -1, "strength": round(strength, 1), "trend_label": "高周期偏空"}
    else:
        return {"direction": 0, "strength": 0, "trend_label": "高周期震荡"}

def calc_volume_profile(candles):
    p = {}
    for c in candles:
        h, l, cl, v = c.get("high", 0), c.get("low", 0), c.get("close", 0), c.get("volume", 0)
        if h <= 0 or v <= 0:
            continue
        tp = round((h + l + cl) / 3, 0)
        p[tp] = p.get(tp, 0) + v
    return p

def calc_vp(candles, tick_size=1.0):
    """改进版Volume Profile: 三角分布法 + Value Area
    将每根K线的成交量按三角分布分配到价格区间，计算POC/VAH/VAL"""
    if not candles:
        return {}
    p = {}
    for c in candles:
        h = c.get("high", 0)
        l = c.get("low", 0)
        cl = c.get("close", 0)
        o = c.get("open", 0)
        vol = c.get("volume", 0)
        if h <= 0 or l <= 0 or vol <= 0:
            continue
        tp = (h + l + cl) / 3.0
        rng = h - l
        if rng <= 0:
            key = round(l / tick_size) * tick_size
            p[key] = p.get(key, 0) + vol
            continue
        low_bin = int(l / tick_size)
        high_bin = int(h / tick_size) + 1
        for b in range(low_bin, high_bin + 1):
            price = b * tick_size
            if price < l or price > h:
                continue
            if price <= tp:
                w = (price - l) / max(tp - l, tick_size * 0.5)
            else:
                w = (h - price) / max(h - tp, tick_size * 0.5)
            w = max(0, w)
            p[price] = p.get(price, 0) + vol * w
    # Value Area (70%)
    if not p:
        return {}
    total_vol = sum(p.values())
    sorted_prices = sorted(p.keys())
    poc_price = max(p, key=p.get)
    poc_idx = sorted_prices.index(poc_price) if poc_price in sorted_prices else 0
    va_vol = p.get(poc_price, 0)
    lo_idx = poc_idx
    hi_idx = poc_idx
    target = total_vol * 0.70
    while va_vol < target and (lo_idx > 0 or hi_idx < len(sorted_prices) - 1):
        add_lo = p.get(sorted_prices[lo_idx - 1], 0) if lo_idx > 0 else -1
        add_hi = p.get(sorted_prices[hi_idx + 1], 0) if hi_idx < len(sorted_prices) - 1 else -1
        if add_lo >= add_hi and add_lo >= 0:
            lo_idx -= 1
            va_vol += add_lo
        elif add_hi >= 0:
            hi_idx += 1
            va_vol += add_hi
        else:
            break
    return {
        "profile": {k: v for k, v in p.items() if v > 0},
        "poc": poc_price,
        "poc_vol": p.get(poc_price, 0),
        "va_high": sorted_prices[hi_idx] if hi_idx < len(sorted_prices) else h,
        "va_low": sorted_prices[lo_idx] if lo_idx >= 0 else l,
        "total_vol": total_vol,
    }

def analyze_vpo(candles):
    """改进版量价分析: 连续评分 + 量价背离检测"""
    if not candles or len(candles) < 5:
        return {"state": "数据不足", "score": 0, "vol_ratio": 0, "price_change": 0,
                "divergence": "none", "div_score": 0}

    recent = candles[-5:]
    older = candles[-20:-5] if len(candles) >= 20 else candles[:-5]
    if not older:
        older = candles[:max(1, len(candles) // 2)]

    avo = sum(c.get("volume", 0) for c in older) / max(len(older), 1)
    avr = sum(c.get("volume", 0) for c in recent) / max(len(recent), 1)
    vr = avr / max(avo, 1) if avo > 0 else 1.0
    vr = max(0.2, min(5.0, vr))

    pf = older[-1].get("close", 0) if older else recent[0].get("close", 0)
    pc = recent[-1].get("close", 0) - pf
    pc_pct = pc / max(pf, 1) * 100 if pf > 0 else 0

    older_pc = older[-1].get("close", 0) - older[0].get("close", 0) if len(older) >= 2 else 0

    # 量价背离检测
    divergence = "none"
    div_score = 0

    if vr > 1.2 and abs(pc_pct) < 0.15:
        divergence = "vol_up_price_flat"
        div_score = -20  # 天量滞涨，危险信号
    elif vr > 1.3 and pc_pct > 0.3:
        divergence = "vol_price_up"
        div_score = min(40, vr * 15)  # 健康放量上涨
    elif vr > 1.3 and pc_pct < -0.3:
        divergence = "vol_price_down"
        div_score = max(-40, -vr * 15)  # 放量下跌
    elif vr < 0.7 and pc_pct > 0.3:
        divergence = "price_up_vol_down"
        div_score = -25  # 缩量上涨，量价背离！
    elif vr < 0.7 and pc_pct < -0.3:
        divergence = "price_down_vol_down"
        div_score = 15  # 缩量下跌，空头衰竭
    elif 0.8 <= vr <= 1.2 and abs(pc_pct) < 0.2:
        divergence = "consolidation"
        div_score = 0
    elif pc_pct > 0.2 and vr >= 0.8:
        divergence = "normal_up"
        div_score = min(30, pc_pct * 20 + (vr - 1) * 15)
    elif pc_pct < -0.2 and vr >= 0.8:
        divergence = "normal_down"
        div_score = max(-30, pc_pct * 20 - (vr - 1) * 15)

    score = max(-100, min(100, div_score))

    state_map = {
        "vol_price_up": "放量上涨",
        "vol_price_down": "放量下跌",
        "price_up_vol_down": "缩量上涨(背离)",
        "price_down_vol_down": "缩量下跌",
        "vol_up_price_flat": "放量滞涨",
        "consolidation": "缩量整理",
        "normal_up": "温和上涨",
        "normal_down": "温和下跌",
    }

    return {
        "state": state_map.get(divergence, "量价分析中"),
        "score": round(score, 1),
        "vol_ratio": round(vr, 2),
        "price_change": round(pc, 1),
        "divergence": divergence,
        "div_score": round(div_score, 1),
    }

def analyze_momentum(candles):
    """改进版动量: 价格ROC + 方向一致性，不再复制delta"""
    if not candles or len(candles) < 5:
        return {"trend": "数据不足", "score": 0, "ratio": 1.0,
                "consistency": 0, "price_roc": 0, "buy_energy": 0, "sell_energy": 0}

    short = candles[-5:]
    long_ = candles[-20:] if len(candles) >= 20 else candles

    # 价格ROC
    sc = short[-1].get("close", 0)
    so = short[0].get("close", 0) if short else 0
    lc = long_[-1].get("close", 0)
    lo = long_[0].get("close", 0) if long_ else 0

    s_roc = (sc - so) / max(so, 1) * 100
    l_roc = (lc - lo) / max(lo, 1) * 100

    # 方向一致性: 连续同向K线占比
    if len(short) >= 3:
        ups = sum(1 for c in short if c.get("close", 0) > c.get("open", 0))
        downs = len(short) - ups
        if s_roc > 0:
            consistency = ups / len(short)
        elif s_roc < 0:
            consistency = downs / len(short)
        else:
            consistency = 0.5
    else:
        consistency = 0.5

    # K线实体占比（过滤长影线噪音）
    bodies = [abs(c.get("close", 0) - c.get("open", 0)) for c in short]
    ranges = [max(c.get("high", 0) - c.get("low", 0), 0.001) for c in short]
    body_ratio = sum(bodies) / max(sum(ranges), 0.001)

    # 趋势分类
    if consistency >= 0.7 and abs(s_roc) > 0.1:
        tr = "强趋势上涨" if s_roc > 0 else "强趋势下跌"
    elif consistency >= 0.5 and abs(s_roc) > 0.05:
        tr = "弱趋势上涨" if s_roc > 0 else "弱趋势下跌"
    elif abs(s_roc) > 0.1 and body_ratio < 0.4:
        tr = "趋势背离"
    else:
        tr = "震荡"

    # 评分: 价格动量70% + 一致性30%
    price_score = max(-60, min(60, s_roc * 300))
    if s_roc > 0:
        cons_score = (consistency - 0.5) * 80
    else:
        cons_score = -(consistency - 0.5) * 80
    score = max(-100, min(100, price_score * 0.7 + cons_score * 0.3))

    # 买卖能量（保留兼容）
    be = sum(max(0, c.get("close", 0) - c.get("open", 0)) * c.get("volume", 0) / max(c.get("high", 0) - c.get("low", 0), 1) for c in short)
    se = sum(max(0, c.get("open", 0) - c.get("close", 0)) * c.get("volume", 0) / max(c.get("high", 0) - c.get("low", 0), 1) for c in short)
    r = be / max(se, 1) if se > 0 else (be if be > 0 else 1.0)

    return {
        "trend": tr,
        "score": round(score, 1),
        "ratio": round(r, 2),
        "consistency": round(consistency, 2),
        "price_roc": round(s_roc, 3),
        "buy_energy": round(be, 1),
        "sell_energy": round(se, 1),
    }

def find_key_levels(vp_full, cp):
    """改进版关键价位: Value Area + 渐进评分"""
    if not vp_full:
        return {"support": None, "resistance": None, "poc": None,
                "va_high": None, "va_low": None, "score": 0}

    poc_price = vp_full.get("poc")
    poc_vol = vp_full.get("poc_vol", 0)
    va_high = vp_full.get("va_high")
    va_low = vp_full.get("va_low")
    profile = vp_full.get("profile", {})

    # 支撑/阻力: VA边界 + 高成交量节点
    support = None
    resistance = None
    if va_low is not None and va_low < cp:
        support = {"price": va_low, "volume": profile.get(va_low, 0)}
    elif profile:
        below = [(p, v) for p, v in profile.items() if p < cp]
        if below:
            bp = max(below, key=lambda x: x[1])
            support = {"price": bp[0], "volume": bp[1]}

    if va_high is not None and va_high > cp:
        resistance = {"price": va_high, "volume": profile.get(va_high, 0)}
    elif profile:
        above = [(p, v) for p, v in profile.items() if p > cp]
        if above:
            ap = max(above, key=lambda x: x[1])
            resistance = {"price": ap[0], "volume": ap[1]}

    # 渐进评分: 相对距离，不再是硬阈值
    score = 0
    if cp > 0:
        if support and support["price"] > 0:
            dist_s = abs(cp - support["price"]) / cp * 100
            if dist_s < 0.3:
                score = 60
            elif dist_s < 0.6:
                score = 35
            elif dist_s < 1.0:
                score = 15
        if resistance and resistance["price"] > 0:
            dist_r = abs(resistance["price"] - cp) / cp * 100
            if dist_r < 0.3:
                score = -60
            elif dist_r < 0.6:
                score = -35
            elif dist_r < 1.0:
                score = -15
        # VA内部 = 中性偏正
        if va_low and va_high and va_low <= cp <= va_high:
            if score == 0:
                score = 10

    return {
        "poc": {"price": poc_price, "volume": poc_vol} if poc_price else None,
        "support": support,
        "resistance": resistance,
        "va_high": va_high,
        "va_low": va_low,
        "score": score,
    }

def _match_rankings_symbol(instrument_id):
    """将instrument_id匹配到rankings数据中的symbol"""
    for cache_key in state.rankings_cache:
        cached = state.rankings_cache[cache_key]
        if not isinstance(cached, dict):
            continue
        data = cached.get("data", {})
        if not isinstance(data, dict):
            continue
        for exchange, items in data.items():
            if not isinstance(items, list):
                continue
            symbols = set()
            for item in items:
                s = item.get("symbol", "")
                if s:
                    symbols.add(s)
            # 精确匹配
            if instrument_id in symbols:
                return items
            # CZCE: instrument_id可能是FG2609, rankings symbol是FG609(3位)
            if len(instrument_id) >= 4:
                prefix = ""
                digits = ""
                for c in instrument_id:
                    if c.isalpha():
                        prefix += c
                    else:
                        digits += c
                if len(digits) == 4:
                    short_id = prefix + digits[1:]
                    if short_id in symbols:
                        return items
    return []

def analyze_holdings(instrument_id):
    """分析机构持仓，返回评分 -100 到 100"""
    items = _match_rankings_symbol(instrument_id)
    if not items:
        return {"score": 0, "state": "无持仓数据", "net_bias": 0, "change_bias": 0,
                "total_long": 0, "total_short": 0, "long_chg": 0, "short_chg": 0}

    total_long = sum(r.get("long_pos", 0) for r in items)
    total_short = sum(r.get("short_pos", 0) for r in items)
    total_long_chg = sum(r.get("long_chg", 0) for r in items)
    total_short_chg = sum(r.get("short_chg", 0) for r in items)

    denom = max(total_long, total_short, 1)
    net_bias = (total_long - total_short) / denom * 100

    # 增减仓方向评分
    chg_net = total_long_chg - total_short_chg
    chg_denom = max(abs(total_long_chg) + abs(total_short_chg), 1)
    change_bias = chg_net / chg_denom * 100

    # 综合: 净持仓偏向60% + 增减仓方向40%
    score = net_bias * 0.6 + change_bias * 0.4
    score = max(-100, min(100, score))

    # 描述
    if net_bias > 20 and change_bias > 10:
        desc = "机构偏多+增仓做多"
    elif net_bias > 20:
        desc = "机构偏多"
    elif net_bias < -20 and change_bias < -10:
        desc = "机构偏空+增仓做空"
    elif net_bias < -20:
        desc = "机构偏空"
    elif change_bias > 20:
        desc = "多头增仓明显"
    elif change_bias < -20:
        desc = "空头增仓明显"
    else:
        desc = "多空均衡"

    return {
        "score": round(score, 1),
        "state": desc,
        "net_bias": round(net_bias, 1),
        "change_bias": round(change_bias, 1),
        "total_long": total_long,
        "total_short": total_short,
        "long_chg": total_long_chg,
        "short_chg": total_short_chg,
    }

def calc_poc_migration(candles, window=10):
    if not candles or len(candles) < window:
        return {"migration": [], "direction": "数据不足", "trend": 0}
    migration = []
    for i in range(window, len(candles) + 1):
        segment = candles[i - window:i]
        profile = calc_volume_profile(segment)
        if profile:
            poc_price = max(profile.items(), key=lambda x: x[1])[0]
            migration.append({"index": i, "price": poc_price, "time": segment[-1].get("time", "")})
    if len(migration) < 2:
        return {"migration": migration, "direction": "数据不足", "trend": 0}
    first_poc = migration[0]["price"]
    last_poc = migration[-1]["price"]
    diff = last_poc - first_poc
    pct_change = diff / first_poc * 100 if first_poc > 0 else 0
    if pct_change > 0.3:
        direction = "上移"
    elif pct_change < -0.3:
        direction = "下移"
    else:
        direction = "横盘"
    return {"migration": migration[-20:], "direction": direction, "trend": round(pct_change, 2)}

def calc_historical_poc(candles_1d, days=5):
    if not candles_1d or len(candles_1d) < 2:
        return []
    result = []
    for candle in candles_1d[-days - 1:]:
        h = candle.get("high", 0)
        l = candle.get("low", 0)
        c = candle.get("close", 0)
        v = candle.get("volume", 0)
        if h <= 0 or l <= 0 or v <= 0:
            continue
        poc = round((h + l + c) / 3, 0)
        result.append({
            "date": candle.get("time", "")[:10],
            "poc": poc, "high": h, "low": l, "close": c, "volume": v
        })
    return result

def calc_macd(candles, fast=12, slow=26, signal=9):
    """计算MACD指标: DIF, DEA, 柱状图(中国惯例×2)"""
    closes = [c["close"] for c in candles]
    if len(closes) < slow:
        return []

    # EMA fast
    k_fast = 2.0 / (fast + 1)
    ema_fast = [closes[0]]
    for i in range(1, len(closes)):
        ema_fast.append(closes[i] * k_fast + ema_fast[-1] * (1 - k_fast))

    # EMA slow
    k_slow = 2.0 / (slow + 1)
    ema_slow = [closes[0]]
    for i in range(1, len(closes)):
        ema_slow.append(closes[i] * k_slow + ema_slow[-1] * (1 - k_slow))

    # DIF
    dif = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]

    # DEA = EMA(signal) of DIF
    k_sig = 2.0 / (signal + 1)
    dea = [dif[0]]
    for i in range(1, len(dif)):
        dea.append(dif[i] * k_sig + dea[-1] * (1 - k_sig))

    result = []
    for i in range(len(candles)):
        t = candles[i].get("time", "")
        result.append({
            "time": t,
            "dif": round(dif[i], 4),
            "dea": round(dea[i], 4),
            "histogram": round((dif[i] - dea[i]) * 2, 4),
        })
    return result

# K线缓存工具
def _cache_key(instrument_id, timeframe):
    return f"{instrument_id}_{timeframe}.json"

def save_kline_cache(instrument_id, timeframe, candles):
    try:
        fp = os.path.join(KLINE_CACHE_DIR, _cache_key(instrument_id, timeframe))
        data = {"updated": datetime.now().isoformat(), "candles": candles}
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        print(f"Cache save error: {e}")

def load_kline_cache(instrument_id, timeframe, max_age_hours=24):
    try:
        fp = os.path.join(KLINE_CACHE_DIR, _cache_key(instrument_id, timeframe))
        if not os.path.exists(fp):
            return None
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
        updated = datetime.fromisoformat(data["updated"])
        if (datetime.now() - updated).total_seconds() > max_age_hours * 3600:
            return None
        return data.get("candles", [])
    except:
        return None

def analyze_instrument(data, inst, timeframe="1m"):
    """对单个品种做完整分析 - v3多周期确认版"""
    iid = inst["instrument_id"]
    d = data.get(iid, {})
    tf_map = {"1m": "candles_1m", "5m": "candles_5m", "15m": "candles_15m", "1d": "candles_1d"}
    candles_key = tf_map.get(timeframe, "candles_1m")
    candles = d.get(candles_key, [])
    if not candles and timeframe == "15m":
        candles = d.get("candles_5m", []) or d.get("candles_1m", [])
    elif not candles:
        candles = d.get("candles_1m", [])
    tick = d.get("tick", {})
    cp = tick.get("last_price", 0) if isinstance(tick, dict) else 0

    if not candles or not tick or isinstance(tick, str):
        return None

    tick_size = inst.get("price_tick", 1.0)

    ds = calc_delta(candles)
    if len(ds) < 5:
        return None

    vp_full = calc_vp(candles, tick_size)
    lv = find_key_levels(vp_full, cp)
    vpo = analyze_vpo(candles)
    mom = analyze_momentum(candles)
    hs = analyze_holdings(iid)

    # --- 新增: ATR计算 ---
    atr = calc_atr(candles, period=14)
    atr_pct = (atr / max(cp, 1)) * 100 if cp > 0 else 0

    # --- 新增: 高周期趋势 ---
    htf_trend = get_higher_tf_trend(data, inst, timeframe)

    # 综合评分: OF 25% + VP 20% + KL 15% + MS 25% + HS 15%
    total_delta = sum(dd["delta"] for dd in ds[-5:])
    total_vol = sum(dd["volume"] for dd in ds[-5:])
    of_val = max(-100, min(100, total_delta / max(total_vol, 1) * 100))
    vs = vpo["score"]
    kl = lv.get("score", 0)
    ms = mom["score"]
    hsc = hs["score"]

    total = of_val * 0.25 + vs * 0.20 + kl * 0.15 + ms * 0.25 + hsc * 0.15

    # --- 新增: 多周期确认加权/惩罚 ---
    htf_dir = htf_trend.get("direction", 0)
    htf_str = htf_trend.get("strength", 0)
    if htf_dir == 1 and total > 0:
        # 高周期多头 + 当前信号做多 → 加分
        total *= 1.0 + htf_str * 0.001  # 最多加10%
    elif htf_dir == -1 and total < 0:
        # 高周期空头 + 当前信号做空 → 加分
        total *= 1.0 + htf_str * 0.001
    elif htf_dir == 1 and total < 0:
        # 高周期多头但信号做空 → 惩罚(逆高周期)
        total *= 0.85
    elif htf_dir == -1 and total > 0:
        # 高周期空头但信号做多 → 惩罚(逆高周期)
        total *= 0.85

    # 信号确认: 方向一致性(软化)
    if len(ds) >= 3:
        recent_dirs = [1 if dd["delta"] > 0 else -1 for dd in ds[-3:]]
        if len(set(recent_dirs)) == 1:
            total *= 1.1  # 一致方向加10%
        elif recent_dirs[0] != recent_dirs[-1]:
            total *= 0.92  # 仅首尾不一致时轻微惩罚

    # 背离惩罚: 动量和delta方向矛盾
    if of_val > 20 and ms < -20:
        total *= 0.88
    elif of_val < -20 and ms > 20:
        total *= 0.88

    # 量价背离惩罚
    if vpo.get("divergence") == "price_up_vol_down" and total > 20:
        total *= 0.82
    elif vpo.get("divergence") == "vol_up_price_flat" and abs(total) > 30:
        total *= 0.80

    total = max(-100, min(100, total))

    return {
        "instrument_id": iid,
        "label": inst.get("label", iid),
        "tick": tick,
        "delta": ds[-20:],
        "volume_profile": vp_full.get("profile", {}),
        "key_levels": lv,
        "vpo": vpo,
        "momentum": mom,
        "holdings": hs,
        "of_val": round(of_val, 1),
        "vs": vs,
        "kl": kl,
        "ms": ms,
        "hs": hsc,
        "total_score": round(total, 1),
        "macd": calc_macd(candles),
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 3),
        "htf_trend": htf_trend,
    }

# ============================================================================
# 回测引擎
# ============================================================================

def run_backtest(candles, threshold, sl_pct, tp_pct, vol, size=10):
    """回测引擎 - 返回交易列表和权益曲线"""
    if not candles or len(candles) < 30:
        return {"trades": [], "equity": [0], "stats": {}}

    lookback = 20
    n_bars = len(candles)

    # 预计算信号
    signals = [0.0] * n_bars
    for i in range(lookback, n_bars):
        window = candles[i - lookback:i]
        cp = candles[i]["close"]
        ds = calc_delta(window)
        if len(ds) < 5:
            continue
        vp = calc_vp(window, 1.0)
        lv = find_key_levels(vp, cp)
        td = sum(dd["delta"] for dd in ds[-5:])
        tv = sum(dd["volume"] for dd in ds[-5:])
        of_val = max(-100, min(100, td / max(tv, 1) * 100))
        vpo = analyze_vpo(window)
        vs = vpo["score"]
        kl = lv.get("score", 0)
        mom = analyze_momentum(window)
        ms = mom["score"]
        total = of_val * 0.30 + vs * 0.20 + kl * 0.15 + ms * 0.35
        # 信号确认: 方向一致性调整(软化)
        if len(ds) >= 3:
            dirs = [1 if dd["delta"] > 0 else -1 for dd in ds[-3:]]
            if len(set(dirs)) == 1:
                total *= 1.1  # 一致方向加10%
            elif dirs[0] != dirs[-1]:
                total *= 0.92  # 仅首尾不一致时轻微惩罚
        # 背离惩罚(软化，避免多重惩罚叠加)
        if of_val > 20 and ms < -20:
            total *= 0.88
        elif of_val < -20 and ms > 20:
            total *= 0.88
        if vpo.get("divergence") == "price_up_vol_down" and total > 20:
            total *= 0.82
        elif vpo.get("divergence") == "vol_up_price_flat" and abs(total) > 30:
            total *= 0.80
        signals[i] = max(-100, min(100, total * 2.5))

    # 交易模拟 (正信号做多, 负信号做空)
    trades = []
    equity = [0.0]
    position = None
    peak_equity = 0.0
    max_dd = 0.0
    cooldown_until = 0  # 平仓后冷却N根K线再开仓

    for i in range(lookback, n_bars):
        cp = candles[i]["close"]
        ch = candles[i]["high"]
        cl = candles[i]["low"]
        ct = candles[i]["time"]
        total = signals[i]

        # 检查止损止盈
        if position:
            pos_dir = position["dir"]
            pos_entry = position["entry"]
            pos_sl = position["sl"]
            pos_tp = position["tp"]

            if pos_dir == "buy":
                hit_sl = cl <= pos_sl
                hit_tp = ch >= pos_tp
            else:
                hit_sl = ch >= pos_sl
                hit_tp = cl <= pos_tp

            if hit_sl or hit_tp:
                exit_price = pos_sl if hit_sl else pos_tp
                pnl = (exit_price - pos_entry) * vol * size if pos_dir == "buy" else (pos_entry - exit_price) * vol * size
                trades.append({
                    "entry_time": position["time"], "exit_time": ct,
                    "dir": pos_dir, "entry": pos_entry, "exit": exit_price,
                    "pnl": round(pnl, 0), "reason": "止损" if hit_sl else "止盈",
                    "vol": vol
                })
                equity.append(equity[-1] + pnl)
                cur_eq = equity[-1]
                if cur_eq > peak_equity:
                    peak_equity = cur_eq
                dd = peak_equity - cur_eq
                if dd > max_dd:
                    max_dd = dd
                position = None
                cooldown_until = i + 5  # 平仓后冷却5根K线
                continue

            # 分级移动止损
            sl_dist = abs(pos_entry * sl_pct / 100)
            if sl_dist > 0:
                cur_profit = abs(cp - pos_entry)
                profit_ratio = cur_profit / sl_dist  # 当前利润是初始风险的几倍
                if pos_dir == "buy":
                    if profit_ratio >= 2.0:
                        # 利润>=2倍风险: 止损移到 entry + 1.5*sl_dist (锁定大部分利润)
                        new_sl = pos_entry + sl_dist * 1.5
                        if new_sl > pos_sl:
                            position["sl"] = round(new_sl, 1)
                    elif profit_ratio >= 1.0:
                        # 利润>=1倍风险: 止损移到保本
                        new_sl = pos_entry
                        if new_sl > pos_sl:
                            position["sl"] = round(new_sl, 1)
                    elif profit_ratio >= 0.5:
                        # 利润>=0.5倍风险: 止损收紧到 entry - 0.5*sl_dist
                        new_sl = pos_entry - sl_dist * 0.5
                        if new_sl > pos_sl:
                            position["sl"] = round(new_sl, 1)
                else:  # sell
                    if profit_ratio >= 2.0:
                        new_sl = pos_entry - sl_dist * 1.5
                        if new_sl < pos_sl:
                            position["sl"] = round(new_sl, 1)
                    elif profit_ratio >= 1.0:
                        new_sl = pos_entry
                        if new_sl < pos_sl:
                            position["sl"] = round(new_sl, 1)
                    elif profit_ratio >= 0.5:
                        new_sl = pos_entry + sl_dist * 0.5
                        if new_sl < pos_sl:
                            position["sl"] = round(new_sl, 1)

        # 信号确认: 最近3根K线中至少2根方向一致
        sig_dir = "buy" if total > 0 else "sell" if total < 0 else None
        recent_signal_count = 0
        if sig_dir:
            for j in range(max(lookback, i - 2), i + 1):
                s = signals[j]
                if (sig_dir == "buy" and s > 0) or (sig_dir == "sell" and s < 0):
                    recent_signal_count += 1
        confirmed = recent_signal_count >= 2

        # 开仓 (需过冷却期)
        if position is None and i >= cooldown_until and abs(total) >= threshold and confirmed:
            direction = "buy" if total > 0 else "sell"
            sl_d = cp * sl_pct / 100
            tp_d = cp * tp_pct / 100
            sl_price = round(cp - sl_d, 1) if direction == "buy" else round(cp + sl_d, 1)
            tp_price = round(cp + tp_d, 1) if direction == "buy" else round(cp - tp_d, 1)
            position = {"dir": direction, "entry": cp, "time": ct, "sl": sl_price, "tp": tp_price, "signal": total}

        # 反转平仓 (降低阈值到0.6x)
        elif position and ((total > 0 and position["dir"] == "sell") or (total < 0 and position["dir"] == "buy")):
            if abs(total) >= threshold * 0.6 and confirmed:
                pnl = (cp - position["entry"]) * vol * size if position["dir"] == "buy" else (position["entry"] - cp) * vol * size
                trades.append({
                    "entry_time": position["time"], "exit_time": ct,
                    "dir": position["dir"], "entry": position["entry"], "exit": cp,
                    "pnl": round(pnl, 0), "reason": "反转平仓",
                    "vol": vol
                })
                equity.append(equity[-1] + pnl)
                cur_eq = equity[-1]
                if cur_eq > peak_equity:
                    peak_equity = cur_eq
                dd = peak_equity - cur_eq
                if dd > max_dd:
                    max_dd = dd
                position = None
                cooldown_until = i + 3  # 反转平仓冷却3根

        equity.append(equity[-1])
        cur_eq = equity[-1]
        if cur_eq > peak_equity:
            peak_equity = cur_eq
        dd = peak_equity - cur_eq
        if dd > max_dd:
            max_dd = dd

    # 强制平仓
    if position:
        cp = candles[-1]["close"]
        pnl = (cp - position["entry"]) * vol * size if position["dir"] == "buy" else (position["entry"] - cp) * vol * size
        trades.append({
            "entry_time": position["time"], "exit_time": candles[-1]["time"],
            "dir": position["dir"], "entry": position["entry"], "exit": cp,
            "pnl": round(pnl, 0), "reason": "回测结束平仓",
            "vol": vol
        })
        equity.append(equity[-1] + pnl)

    # 统计
    total_pnl = equity[-1] if equity else 0
    n_trades = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    win_rate = len(wins) / n_trades * 100 if n_trades > 0 else 0
    avg_win = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
    profit_factor = abs(sum(t["pnl"] for t in wins) / sum(t["pnl"] for t in losses)) if losses and sum(t["pnl"] for t in losses) != 0 else float("inf")

    return {
        "trades": trades,
        "equity": [round(e, 0) for e in equity],
        "stats": {
            "total_pnl": round(total_pnl, 0),
            "n_trades": n_trades,
            "win_rate": round(win_rate, 1),
            "max_dd": round(max_dd, 0),
            "avg_win": round(avg_win, 0),
            "avg_loss": round(avg_loss, 0),
            "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "inf",
        }
    }

# ============================================================================
# API 路由
# ============================================================================

@app.get("/api/status")
async def get_status():
    return {
        "connected": state.connected,
        "in_trading": is_trading_hours(),
        "next_open": get_next_open_time(),
        "start_time": state.start_time.isoformat() if state.start_time else None,
        "auto_trading": state.auto_trading,
        "instruments": state.instruments,
        "version": "2.1.0",
    }

# ============================================================================
# 检查更新 & 自动更新
# ============================================================================

_UPDATE_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "update_config.json")

def _get_current_version():
    """读取当前版本号"""
    try:
        with open(__file__, 'r', encoding='utf-8') as f:
            for line in f:
                if '"version"' in line and re.search(r'"version"\s*:\s*"(\d+\.\d+\.\d+)"', line):
                    return re.search(r'"version"\s*:\s*"(\d+\.\d+\.\d+)"', line).group(1)
    except:
        pass
    return "2.0.0"

@app.get("/api/check_update")
async def check_update():
    """检查GitHub上的最新版本"""
    try:
        with open(_UPDATE_CONFIG, 'r', encoding='utf-8') as f:
            config = json.load(f)
        update_url = config.get("update_url", "")
        if not update_url:
            return {"has_update": False, "error": "no update_url configured"}

        resp = requests.get(update_url, timeout=10)
        if resp.status_code != 200:
            return {"has_update": False, "error": f"HTTP {resp.status_code}"}

        remote = resp.json()
        remote_ver = remote.get("version", "0.0.0")
        local_ver = _get_current_version()

        # 比较版本号
        def ver_tuple(v):
            try:
                return tuple(int(x) for x in v.split('.'))
            except:
                return (0, 0, 0)

        has_update = ver_tuple(remote_ver) > ver_tuple(local_ver)
        return {
            "has_update": has_update,
            "local_version": local_ver,
            "remote_version": remote_ver,
            "changelog": remote.get("changelog", ""),
            "update_time": remote.get("update_time", ""),
            "files": remote.get("files", {}),
        }
    except Exception as e:
        return {"has_update": False, "error": str(e)}

@app.post("/api/do_update")
async def do_update():
    """下载更新文件并应用（通过辅助bat重启服务器）"""
    try:
        with open(_UPDATE_CONFIG, 'r', encoding='utf-8') as f:
            config = json.load(f)
        update_url = config.get("update_url", "")

        # 获取远程update.json
        resp = requests.get(update_url, timeout=10)
        if resp.status_code != 200:
            return {"ok": False, "error": f"获取update.json失败: HTTP {resp.status_code}"}

        remote = resp.json()
        files = remote.get("files", {})
        if not files:
            return {"ok": False, "error": "远程没有需要更新的文件"}

        internal_dir = os.path.dirname(os.path.abspath(__file__))
        staging_dir = os.path.join(internal_dir, "_update_staging")
        os.makedirs(staging_dir, exist_ok=True)

        # 下载所有更新文件
        downloaded = []
        for filename, url in files.items():
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                fp = os.path.join(staging_dir, filename)
                # index.html 放在 static/ 子目录
                if filename == "index.html":
                    os.makedirs(os.path.join(staging_dir, "static"), exist_ok=True)
                    fp = os.path.join(staging_dir, "static", "index.html")
                with open(fp, 'wb') as f:
                    f.write(r.content)
                downloaded.append(filename)
            else:
                return {"ok": False, "error": f"下载 {filename} 失败: HTTP {r.status_code}"}

        if not downloaded:
            return {"ok": False, "error": "没有文件下载成功"}

        # 创建辅助bat: 等服务器退出 → 复制文件 → 重启服务器
        # 找到启动器
        app_dir = os.path.dirname(internal_dir)
        exe_path = os.path.join(app_dir, "dashboard_app.exe")
        start_script = os.path.join(app_dir, "start_server.py")

        # 确定用哪个启动
        if os.path.exists(exe_path):
            restart_cmd = f'start "" "{exe_path}"'
        elif os.path.exists(start_script):
            restart_cmd = f'start "" pythonw "{start_script}"'
        else:
            restart_cmd = f'start "" python "{os.path.join(internal_dir, "server.py")}"'

        bat_content = f"""@echo off
chcp 65001 >nul 2>&1
echo Waiting for server to stop...
timeout /t 3 /nobreak >nul

:: 等待端口释放
:waitloop
netstat -ano | findstr ":8501" >nul 2>&1
if %errorlevel%==0 (
    timeout /t 2 /nobreak >nul
    goto waitloop
)

echo Applying update...
:: 复制更新文件
"""
        for filename in downloaded:
            if filename == "index.html":
                src = os.path.join(staging_dir, "static", "index.html")
                dst = os.path.join(internal_dir, "static", "index.html")
            else:
                src = os.path.join(staging_dir, filename)
                dst = os.path.join(internal_dir, filename)
            bat_content += f'copy /y "{src}" "{dst}" >nul\n'

        bat_content += f"""
:: 清理临时文件
rmdir /s /q "{staging_dir}" >nul 2>&1

echo Restarting server...
{restart_cmd}

:: 删除自身
del "%~f0" >nul 2>&1
"""
        bat_path = os.path.join(internal_dir, "_apply_update.bat")
        with open(bat_path, 'w', encoding='gbk') as f:
            f.write(bat_content)

        # 启动bat（ detached ）
        import subprocess
        subprocess.Popen(
            [bat_path],
            creationflags=0x00000008 | 0x00000200,  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
            cwd=internal_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        return {
            "ok": True,
            "version": remote.get("version", ""),
            "files": downloaded,
            "message": f"更新已下载，服务器将在3秒后重启以应用更新"
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/instruments")
async def get_instruments():
    return {"instruments": state.instruments}

@app.post("/api/instruments")
async def set_instruments(data: dict):
    old_ids = [i.get("instrument_id") for i in state.instruments]
    state.instruments = data.get("instruments", [])
    new_ids = [i.get("instrument_id") for i in state.instruments]
    state.save_state()
    # 品种变化时重启TQ流
    if old_ids != new_ids:
        feed = TQFeed.get()
        feed.stop()
        _dlog(f"Instruments changed, TQ stopped. background_refresh will reconnect.")
    return {"ok": True}

@app.get("/api/data")
async def get_data():
    """获取所有品种的最新数据"""
    return {"data": state.data, "connected": state.connected, "tq_connected": state.tq_connected, "tq_error": state.tq_error}

@app.post("/api/refresh")
async def refresh_data():
    """强制刷新数据 - TQ优先，MCP备用"""
    # 优先TQ
    feed = TQFeed.get()
    if feed._connected and state.instruments:
        data = await asyncio.to_thread(feed._build_result)
        if data:
            state.data = _merge_data(state.data, data)
            state.connected = True
            if not state.start_time:
                state.start_time = datetime.now()
            return {"ok": True, "source": "tq"}
    # 回退MCP
    if state.connected and state.instruments:
        mcp_data = await asyncio.to_thread(mcp_client.fetch_data, state.instruments, True)
        if mcp_data:
            state.data = _merge_data(state.data, mcp_data)
            state.connected = True
            if not state.start_time:
                state.start_time = datetime.now()
            return {"ok": True, "source": "mcp"}
    return {"ok": False, "error": "数据获取失败"}

@app.get("/api/analysis/{instrument_id}")
async def get_analysis(instrument_id: str, timeframe: str = "1m"):
    """获取单品种分析"""
    inst = next((i for i in state.instruments if i["instrument_id"] == instrument_id), None)
    if not inst:
        return {"error": "品种不存在"}
    result = analyze_instrument(state.data, inst, timeframe)
    return result or {"error": "数据不足"}

@app.get("/api/analysis_all")
async def get_analysis_all(timeframe: str = "1m"):
    """获取所有品种分析"""
    results = []
    for inst in state.instruments:
        r = analyze_instrument(state.data, inst, timeframe)
        if r:
            results.append(r)
    return {"analysis": results}

def _run_backtest_sync(instrument_id, timeframe, threshold, sl_pct, tp_pct, vol, size, start_date="", end_date=""):
    """同步回测逻辑 (在线程池中运行)"""
    inst = next((i for i in state.instruments if i["instrument_id"] == instrument_id), None)
    if not inst:
        return {"error": "品种不存在"}

    candles = []
    tf_style = {"1m": "M1", "5m": "M5", "15m": "M15", "1h": "H1", "1d": "D1"}
    style = tf_style.get(timeframe, "M5")

    # 优先从MCP拉取3000根历史K线（数据最完整）
    try:
        mcp_result = _mcp_trade_call("fetch_candles", {
            "exchange": inst["exchange"],
            "instrument_id": instrument_id,
            "style": style,
            "count": -1500,
            "simply": True
        })
        if mcp_result.get("success"):
            raw = mcp_result.get("result", [])
            if isinstance(raw, list):
                for c in raw:
                    # MCP可能返回JSON字符串或dict
                    if isinstance(c, str):
                        try:
                            c = json.loads(c)
                        except:
                            continue
                    if not isinstance(c, dict):
                        continue
                    o = c.get("open", 0)
                    h = c.get("high", 0)
                    l = c.get("low", 0)
                    cl = c.get("close", 0)
                    v = c.get("volume", 0)
                    dt = c.get("datetime", c.get("time", ""))
                    if o <= 0 or cl <= 0 or v <= 0:
                        continue
                    # 统一时间格式
                    if isinstance(dt, str) and "T" in dt:
                        t_str = dt.replace("T", " ")[:16]
                    elif isinstance(dt, str):
                        t_str = dt[:16]
                    else:
                        t_str = str(dt)
                    candles.append({"time": t_str, "open": float(o), "high": float(h), "low": float(l), "close": float(cl), "volume": int(v)})
                print(f"[Backtest] MCP fetched {len(candles)} {style} candles for {instrument_id}")
                if candles:
                    save_kline_cache(instrument_id, timeframe, candles)
    except Exception as e:
        print(f"[Backtest] MCP fetch error: {e}")

    # MCP失败，降级到天勤
    if len(candles) < 100:
        candles = []
        sym = _to_tq_symbol(inst["instrument_id"], inst["exchange"])
        tf_sec = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
        sec = tf_sec.get(timeframe, 300)
        api, err = _get_tq_api()
        if not err and api:
            try:
                klines = api.get_kline_serial(sym, sec, data_length=8900)
                api.wait_update(deadline=time.time() + 20)
                import math
                for _, row in klines.iterrows():
                    o = float(row.get("open", 0))
                    h = float(row.get("high", 0))
                    l = float(row.get("low", 0))
                    c = float(row.get("close", 0))
                    v = float(row.get("volume", 0))
                    if any(math.isnan(x) for x in [o, h, l, c, v]):
                        continue
                    if o <= 0 or v <= 0:
                        continue
                    t_raw = row.get("datetime", 0)
                    if isinstance(t_raw, (int, float)) and not math.isnan(t_raw) and t_raw > 0:
                        ts = t_raw / 1e9 if t_raw > 1e15 else t_raw
                        t_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
                    else:
                        t_str = str(t_raw)
                    candles.append({"time": t_str, "open": o, "high": h, "low": l, "close": c, "volume": int(v)})
                if candles:
                    save_kline_cache(instrument_id, timeframe, candles)
                print(f"[Backtest] TQ fetched {len(candles)} candles for {instrument_id}")
            except Exception as e:
                print(f"[Backtest] TQ fetch error: {e}")

    # 都失败则用缓存
    if len(candles) < 30:
        cached = load_kline_cache(instrument_id, timeframe)
        if cached and len(cached) >= 30:
            candles = cached
            print(f"[Backtest] Using cache: {len(candles)} candles for {instrument_id}")

    if not candles or len(candles) < 30:
        return {"error": f"K线数据不足 ({len(candles)}根)"}

    # 按日期范围过滤
    if start_date:
        candles = [c for c in candles if c["time"][:10] >= start_date[:10]]
    if end_date:
        candles = [c for c in candles if c["time"][:10] <= end_date[:10]]

    if len(candles) < 30:
        return {"error": f"日期过滤后K线不足 ({len(candles)}根)"}

    result = run_backtest(candles, threshold, sl_pct, tp_pct, vol, size)
    result["candle_count"] = len(candles)
    result["instrument_id"] = instrument_id
    result["timeframe"] = timeframe
    result["start_date"] = candles[0].get("time", "") if candles else ""
    result["end_date"] = candles[-1].get("time", "") if candles else ""
    return result

@app.post("/api/backtest")
async def backtest(data: dict):
    """运行回测"""
    instrument_id = data.get("instrument_id")
    timeframe = data.get("timeframe", "5m")
    threshold = data.get("threshold", 20)
    sl_pct = data.get("sl_pct", 1.0)
    tp_pct = data.get("tp_pct", 2.5)
    vol = data.get("vol", 1)
    size = data.get("size", 10)
    start_date = data.get("start_date", "")
    end_date = data.get("end_date", "")

    result = await asyncio.to_thread(
        _run_backtest_sync, instrument_id, timeframe, threshold, sl_pct, tp_pct, vol, size, start_date, end_date
    )
    return result

@app.get("/api/capital")
async def get_capital():
    """获取账户资金（实时从MCP拉取）"""
    result = await asyncio.to_thread(_mcp_trade_call, "fetch_capital", {})
    if result.get("success"):
        capital = result.get("result", {})
        state.capital_info = capital
        return {"capital": capital, "ok": True}
    return {"capital": state.capital_info, "ok": False, "error": result.get("error", "获取失败")}

@app.get("/api/holdings")
async def get_holdings():
    """获取持仓（实时从MCP拉取）"""
    result = await asyncio.to_thread(_mcp_trade_call, "fetch_all_holdings", {"simple": True})
    if not result.get("success"):
        return {"holdings": state.holdings_info, "ok": False, "error": result.get("error", "获取失败")}

    raw = result.get("result", {})
    positions = []
    if isinstance(raw, dict):
        for investor, contracts in raw.items():
            if not isinstance(contracts, dict):
                continue
            for iid, hedgeflags in contracts.items():
                if not isinstance(hedgeflags, dict):
                    continue
                for hf, pos_raw in hedgeflags.items():
                    # pos_raw might be a string representation of dict
                    if isinstance(pos_raw, str):
                        try:
                            pos_raw = eval(pos_raw)
                        except:
                            continue
                    if not isinstance(pos_raw, dict):
                        continue
                    for side in ["long", "short"]:
                        info = pos_raw.get(side, {})
                        vol = info.get("position", 0)
                        if vol > 0:
                            positions.append({
                                "instrument_id": iid,
                                "direction": side,
                                "volume": vol,
                                "avg_price": info.get("position_avg_price", 0) or info.get("open_avg_price", 0),
                                "profit": info.get("position_profit", 0),
                                "margin": info.get("used_margin", 0),
                                "close_profit": info.get("close_profit", 0),
                                "exchange": info.get("exchange", ""),
                            })
    state.holdings_info = positions
    state.positions = positions
    return {"holdings": positions, "ok": True}

@app.get("/api/config")
async def get_config():
    return {
        "signal_threshold": state.signal_threshold,
        "reversal_threshold": state.reversal_threshold,
        "sl_pct": state.sl_pct,
        "tp_pct": state.tp_pct,
        "trade_volume": state.trade_volume,
        "timeframe": state.timeframe,
        "auto_trading": state.auto_trading,
        "instrument_configs": state.instrument_configs,
    }

@app.post("/api/config")
async def set_config(data: dict):
    if "signal_threshold" in data:
        state.signal_threshold = data["signal_threshold"]
    if "reversal_threshold" in data:
        state.reversal_threshold = data["reversal_threshold"]
    if "sl_pct" in data:
        state.sl_pct = data["sl_pct"]
    if "tp_pct" in data:
        state.tp_pct = data["tp_pct"]
    if "trade_volume" in data:
        state.trade_volume = data["trade_volume"]
    if "timeframe" in data:
        state.timeframe = data["timeframe"]
    if "auto_trading" in data:
        state.auto_trading = data["auto_trading"]
    if "tq_user" in data:
        state.tq_user = data["tq_user"]
    if "tq_pass" in data:
        state.tq_pass = data["tq_pass"]
    state.save_state()
    # 如果保存了天勤账号，自动尝试连接
    tq_result = None
    if "tq_user" in data or "tq_pass" in data:
        tq_result = await asyncio.to_thread(connect_tq)
    return {"ok": True, "tq": tq_result}

@app.post("/api/instrument_config")
async def set_instrument_config(data: dict):
    """设置品种独立配置"""
    iid = data.get("instrument_id")
    if not iid:
        return {"error": "缺少instrument_id"}
    if iid not in state.instrument_configs:
        state.instrument_configs[iid] = {}
    ic = state.instrument_configs[iid]
    for key in ["threshold", "reversal", "sl_pct", "tp_pct", "trade_volume"]:
        if key in data:
            ic[key] = data[key]
    state.save_state()
    return {"ok": True, "instrument_id": iid, "config": ic}

@app.get("/api/instrument_configs")
async def get_instrument_configs():
    """获取所有品种独立配置"""
    result = {}
    for inst in state.instruments:
        iid = inst["instrument_id"]
        result[iid] = get_inst_config(iid)
    return result

# ============================================================================
# 天勤连接 API
# ============================================================================

@app.get("/api/tq_status")
async def tq_status():
    """获取天勤连接状态"""
    return {
        "connected": state.tq_connected,
        "error": state.tq_error,
        "has_credentials": bool(getattr(state, 'tq_user', None) and getattr(state, 'tq_pass', None)),
    }

@app.post("/api/tq_connect")
async def tq_connect():
    """手动连接天勤"""
    result = await asyncio.to_thread(connect_tq)
    return result

@app.post("/api/tq_disconnect")
async def tq_disconnect():
    """断开天勤连接"""
    result = await asyncio.to_thread(disconnect_tq)
    return result

# ============================================================================
# 交易 & 排名 API
# ============================================================================

@app.post("/api/trade")
async def trade(data: dict):
    """手动下单"""
    iid = data.get("instrument_id")
    ex = data.get("exchange")
    direction = data.get("direction")  # buy/sell
    volume = int(data.get("volume", 1))
    price = float(data.get("price", 0))
    if not all([iid, ex, direction]):
        return {"ok": False, "error": "参数缺失"}
    # 如果未指定价格，从缓存取最新价
    if price <= 0:
        tick = state.data.get(iid, {}).get("tick", {})
        price = tick.get("last_price", 0) if isinstance(tick, dict) else 0
    if price <= 0:
        return {"ok": False, "error": "无法获取当前价格，请先刷新数据"}
    result = await asyncio.to_thread(submit_trade, iid, ex, direction, volume, price)
    return {**result, "ok": result.get("success", False)}

@app.post("/api/close_position")
async def api_close_position(data: dict):
    """手动平仓"""
    iid = data.get("instrument_id")
    ex = data.get("exchange")
    direction = data.get("direction")
    volume = int(data.get("volume", 1))
    reason = data.get("reason", "手动平仓")
    if not all([iid, ex, direction]):
        return {"ok": False, "error": "参数缺失"}
    result = await asyncio.to_thread(close_position, iid, ex, direction, volume, reason)
    return {**result, "ok": result.get("success", False)}

@app.get("/api/trade_log")
async def get_trade_log():
    """获取交易日志"""
    return {"log": state.trade_log}

@app.get("/api/rankings")
async def get_rankings(exchange: str = None):
    """获取机构持仓排名"""
    data = await asyncio.to_thread(fetch_rankings_sync, exchange)
    return {"rankings": data}

@app.post("/api/auto_trading")
async def toggle_auto_trading(data: dict):
    """开关自动交易"""
    enabled = bool(data.get("enabled", False))
    state.auto_trading = enabled
    state.save_state()
    _add_trade_log("系统", "-", reason=f"自动交易{'开启' if enabled else '关闭'}")
    return {"ok": True, "auto_trading": state.auto_trading}

# ============================================================================
# WebSocket 实时推送
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    state.ws_clients.add(websocket)
    try:
        while True:
            # 接收客户端消息 (可选)
            data = await websocket.receive_text()
            # 可以处理客户端请求
    except WebSocketDisconnect:
        state.ws_clients.discard(websocket)

async def broadcast_data():
    """定期广播数据到所有WebSocket客户端"""
    while True:
        if state.ws_clients and state.data:
            # 构建精简的推送数据
            push_data = {
                "type": "quotes",
                "timestamp": datetime.now().isoformat(),
                "in_trading": is_trading_hours(),
                "data": {}
            }
            for inst in state.instruments:
                iid = inst["instrument_id"]
                d = state.data.get(iid, {})
                tick = d.get("tick", {})
                if isinstance(tick, dict) and tick:
                    td = {
                        "last_price": tick.get("last_price", 0),
                        "open": tick.get("open", 0),
                        "high": tick.get("high", 0),
                        "low": tick.get("low", 0),
                        "pre_close": tick.get("pre_close", 0),
                        "volume": tick.get("volume", 0),
                        "upper_limit": tick.get("upper_limit", 0),
                        "lower_limit": tick.get("lower_limit", 0),
                    }
                    # 5档买卖盘
                    for lvl in range(1, 6):
                        td[f"ask_price{lvl}"] = tick.get(f"ask_price{lvl}", 0)
                        td[f"ask_volume{lvl}"] = tick.get(f"ask_volume{lvl}", 0)
                        td[f"bid_price{lvl}"] = tick.get(f"bid_price{lvl}", 0)
                        td[f"bid_volume{lvl}"] = tick.get(f"bid_volume{lvl}", 0)
                    push_data["data"][iid] = td
            msg = json.dumps(push_data, ensure_ascii=False)
            disconnected = set()
            for client in state.ws_clients:
                try:
                    await client.send_text(msg)
                except:
                    disconnected.add(client)
            state.ws_clients -= disconnected

        # 交易时间3秒推送一次, 休市10秒
        await asyncio.sleep(3 if is_trading_hours() else 10)

# ============================================================================
# 后台任务: 定时刷新数据
# ============================================================================

def _merge_data(old_data, new_data, incremental=False):
    """合并新旧数据。incremental=True时，用新数据更新旧数组尾部(增量刷新)。
    incremental=False时，保留更长的K线数组(全量加载)。"""
    if not old_data:
        return new_data
    if not new_data:
        return old_data
    merged = dict(new_data)  # copy new
    for iid, old_inst in old_data.items():
        if iid not in merged:
            merged[iid] = old_inst
            continue
        new_inst = merged[iid]
        for key in ["candles_1m", "candles_5m", "candles_15m", "candles_1d"]:
            old_c = old_inst.get(key, []) or []
            new_c = new_inst.get(key, []) or []
            if not new_c:
                new_inst[key] = old_c
            elif incremental and old_c and len(new_c) < len(old_c):
                # 增量模式: 用新数据替换旧数组尾部
                # 按时间戳匹配，找到重叠部分
                new_times = {c.get("time") or c.get("datetime") for c in new_c if isinstance(c, dict)}
                # 找到旧数组中不在新数据里的部分(保留历史)
                keep = [c for c in old_c if (c.get("time") or c.get("datetime")) not in new_times]
                new_inst[key] = keep + new_c
            elif len(old_c) > len(new_c):
                new_inst[key] = old_c
        # tick总是用新的
    return merged

_DEBUG_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_refresh.log")

def _dlog(msg):
    """写入调试日志到文件"""
    try:
        with open(_DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%H:%M:%S')} {msg}\n")
    except:
        pass

async def background_refresh():
    """后台定时刷新数据 - TQ为主数据源(实时推送)，MCP为备用"""
    _dlog("background_refresh started")
    await asyncio.sleep(3)
    if not state.instruments:
        _dlog(f"no instruments, exiting")
        return

    # === 优先连接TQ ===
    _dlog(f"connecting TQ for {len(state.instruments)} instruments...")
    feed = TQFeed.get()
    t0 = time.time()
    tq_ok = await asyncio.to_thread(feed.connect, state.instruments)
    elapsed = time.time() - t0
    _dlog(f"TQ connect result: {tq_ok} in {elapsed:.1f}s")

    if tq_ok:
        state.connected = True
        # 首次等待TQ数据就绪(3秒让初始数据到达)
        await asyncio.sleep(3)
        # 读取一次完整数据
        initial = await asyncio.to_thread(feed._build_result)
        if initial:
            state.data = _merge_data(state.data, initial)
            sample = list(initial.keys())[:2]
            candle_info = {k: len(initial[k].get('candles_1m') or []) for k in sample}
            _dlog(f"TQ initial data: {len(initial)} instruments, M1 sample: {candle_info}")

        # 启动TQ流式推送线程(持续wait_update循环)
        feed.start_streaming(state.instruments)
        _dlog("TQ streaming started, monitoring...")

        # 主循环：监控TQ连接状态，必要时重连
        while True:
            await asyncio.sleep(10)
            if not feed._connected and state.instruments:
                _dlog("TQ disconnected, attempting reconnect...")
                ok = await asyncio.to_thread(feed.connect, state.instruments)
                if ok:
                    feed.start_streaming(state.instruments)
                    _dlog("TQ reconnected and streaming resumed")
            state.connected = feed._connected
    else:
        # === TQ失败，回退MCP ===
        _dlog("TQ failed, falling back to MCP...")
        ok = await asyncio.to_thread(mcp_client.connect)
        _dlog(f"MCP connect result: {ok}")
        if not ok:
            await asyncio.sleep(5)
            ok = await asyncio.to_thread(mcp_client.connect, True)
            _dlog(f"MCP retry result: {ok}")
        if ok:
            state.connected = True
            await asyncio.to_thread(mcp_client.ensure_subscribed, state.instruments)
            _dlog("MCP subscribed, starting fetch loops")
            await asyncio.gather(_tick_refresh_loop(), _candle_refresh_loop())
        else:
            _dlog("Both TQ and MCP failed, no data source available")
            state.connected = False

async def _tick_refresh_loop():
    """高频tick刷新 - 每10秒更新价格"""
    _dlog("tick loop started")
    cycle = 0
    while True:
        try:
            if state.connected and state.instruments:
                t0 = time.time()
                tick_data = await asyncio.to_thread(
                    mcp_client.fetch_ticks_only, state.instruments
                )
                elapsed = time.time() - t0
                cycle += 1
                if tick_data:
                    state.data = _merge_data(state.data, tick_data)
                    state.connected = True
                    if cycle <= 3:
                        _dlog(f"tick cycle {cycle}: {len(tick_data)} instruments in {elapsed:.1f}s")
                else:
                    _dlog(f"tick cycle {cycle}: empty result in {elapsed:.1f}s, connected={mcp_client._connected}")
                    if not mcp_client._connected:
                        state.connected = False
                        ok = await asyncio.to_thread(mcp_client.connect, True)
                        _dlog(f"tick reconnect: {ok}")
                        if ok:
                            state.connected = True
                            await asyncio.to_thread(mcp_client.ensure_subscribed, state.instruments)
        except Exception as e:
            _dlog(f"tick error: {e}")
        await asyncio.sleep(10 if is_trading_hours() else 30)

async def _candle_refresh_loop():
    """后台K线刷新 - 每120秒更新K线"""
    await asyncio.sleep(15)  # 首次加载后等一会再开始
    _dlog("candle loop started")
    cycle = 0
    while True:
        try:
            if state.connected and state.instruments:
                t0 = time.time()
                candle_data = await asyncio.to_thread(
                    mcp_client.fetch_candles_only, state.instruments, False
                )
                elapsed = time.time() - t0
                cycle += 1
                if candle_data:
                    sample_k = list(candle_data.keys())[0] if candle_data else None
                    sample_m1 = len(candle_data.get(sample_k, {}).get('candles_1m') or []) if sample_k else 0
                    _dlog(f"candle cycle {cycle}: {len(candle_data)} instruments in {elapsed:.1f}s, M1 sample={sample_m1}")
                    state.data = _merge_data(state.data, candle_data, incremental=True)
                else:
                    _dlog(f"candle cycle {cycle}: empty in {elapsed:.1f}s")
        except Exception as e:
            _dlog(f"candle error: {e}")
        await asyncio.sleep(120 if is_trading_hours() else 300)

# ============================================================================
# 启动
# ============================================================================

@app.on_event("startup")
async def startup_event():
    # 启动后台任务
    asyncio.create_task(background_refresh())
    asyncio.create_task(broadcast_data())
    asyncio.create_task(auto_trade_loop())

# 挂载静态文件 (前端)
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

@app.get("/")
async def root():
    html_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
