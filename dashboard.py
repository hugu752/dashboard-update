"""
订单流量化分析看盘面板
========================
每次刷新一次性连接MCP取数据 (已验证可行方案)
启动: streamlit run dashboard.py
"""

import json
import os
import re
import time
import ast
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st


# ============================================================================
# 版本信息
# ============================================================================

DASHBOARD_VERSION = "1.0.0"
UPDATE_URL = "https://raw.githubusercontent.com/hugu752/dashboard-update/main/update.json"  # 用户可在此填入更新服务器URL或本地/网络路径


# ============================================================================
# 配置
# ============================================================================

MCP_SSE_URL = "http://127.0.0.1:3000/sse"
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_state.json")

# ============================================================================
# 天勤量化 (tqsdk) 数据源
# ============================================================================

def _to_tq_symbol(instrument_id, exchange):
    """将 exchange + instrument_id 转为天勤代码, 如 CZCE.FG2609"""
    iid = instrument_id
    if exchange == "CZCE":
        import re as _re
        m = _re.match(r"^([A-Za-z]+)(\d{3})$", iid)
        if m:
            iid = f"{m.group(1)}2{m.group(2)}"
    return f"{exchange}.{iid}"

# 交易所夜盘时间段 (开始小时, 结束小时) - 用于判断交易时间
# 日盘: 9:00-11:30, 13:30-15:00
# 夜盘: 21:00-次日凌晨 (不同品种结束时间不同)


def is_trading_hours(now=None):
    """判断当前是否在期货交易时间内"""
    if now is None:
        now = datetime.now()
    weekday = now.weekday()  # 0=Monday
    hour = now.hour
    minute = now.minute
    t = hour * 60 + minute

    # 周末不交易
    if weekday >= 5:
        return False

    # 周五没有夜盘 (21:00之后)
    # 注意: 周五夜盘实际是周六凌晨, 但交易所周五晚上不开盘

    # 日盘: 9:00-10:15, 10:30-11:30, 13:30-15:00
    if 540 <= t <= 615:    # 9:00-10:15
        return True
    if 630 <= t <= 690:    # 10:30-11:30
        return True
    if 810 <= t <= 900:    # 13:30-15:00
        return True

    # 夜盘: 21:00-次日02:30 (最晚的品种)
    # 简单判断: 21:00-23:59 或 00:00-02:30
    if t >= 1260:  # 21:00之后
        # 周五没有夜盘
        if weekday == 4:
            return False
        return True
    if t <= 150:   # 00:00-02:30
        # 周六凌晨 = 周五夜盘 → 周五没夜盘
        if weekday == 5:
            return False
        return True

    return False


def get_next_open_time(now=None):
    """返回下一个开盘时间字符串"""
    if now is None:
        now = datetime.now()
    weekday = now.weekday()
    t = now.hour * 60 + now.minute

    # 如果在交易时间内, 返回当天收盘时间
    if is_trading_hours(now):
        if t < 690:
            return "今天11:30"
        elif t < 900:
            return "今天15:00"
        else:
            return "明天09:00"

    # 不在交易时间
    if t < 540:  # 9:00之前
        return "今天09:00"
    elif t < 630:  # 10:30之前 (休息时段)
        return "今天10:30"
    elif t < 810:  # 13:30之前
        return "今天13:30"
    elif t < 1260:  # 21:00之前
        if weekday == 4:  # 周五
            return "下周一09:00"
        return "今天21:00"
    else:
        if weekday == 4:  # 周五夜盘后
            return "下周一09:00"
        return "明天09:00"



# ============================================================================
# 天勤量化 (tqsdk) 数据获取
# ============================================================================

# 注册退出时清理天勤API连接
import atexit as _atexit
def _cleanup_tq():
    try:
        import streamlit as _st
        api = getattr(_st.session_state, '_tq_api', None)
        if api is not None:
            api.close()
    except Exception:
        pass
_atexit.register(_cleanup_tq)

def _get_tq_api():
    """获取或创建天勤API实例 (缓存在session_state中)"""
    try:
        from tqsdk import TqApi, TqAuth
    except ImportError:
        return None, "tqsdk 未安装, 请运行: pip install tqsdk"

    if "_tq_api" in st.session_state and st.session_state._tq_api is not None:
        api = st.session_state._tq_api
        # 检查API连接是否仍然存活
        alive = True
        try:
            if hasattr(api, '_is_alive'):
                alive = api._is_alive
            if alive:
                # 再验证一下内部状态
                try:
                    _ = api._account
                except Exception:
                    alive = False
        except Exception:
            alive = False

        if alive:
            return api, None

        # 连接已断开, 清理旧实例
        try:
            api.close()
        except Exception:
            pass
        st.session_state._tq_api = None
        st.session_state._tq_subs = None

    tq_user = st.session_state.get("tq_user", "")
    tq_pass = st.session_state.get("tq_pass", "")
    if not tq_user or not tq_pass:
        return None, "NEED_AUTH"

    # 使用线程+超时创建API, 防止TqApi()卡死
    _result = {"api": None, "err": None}
    def _create_api():
        try:
            _result["api"] = TqApi(auth=TqAuth(tq_user, tq_pass))
        except Exception as e:
            _result["err"] = e

    t = threading.Thread(target=_create_api, daemon=True)
    t.start()
    t.join(timeout=40)  # 天勤连接通常需要18-30秒

    if t.is_alive():
        # 超时了, 线程还在跑但无法终止
        return None, "天勤连接超时(40秒), 请检查网络后重试"

    if _result["err"]:
        err = str(_result["err"])
        if "auth" in err.lower() or "password" in err.lower():
            return None, "AUTH_FAILED"
        return None, f"天勤连接失败: {err}"

    api = _result["api"]
    if api is None:
        return None, "天勤连接失败: 未知错误"

    st.session_state._tq_api = api
    st.session_state._tq_subs = None  # 新API需要重新订阅
    return api, None


def _get_tq_subscriptions(api, instruments):
    """获取或创建天勤订阅 (缓存在session_state中, 避免重复订阅)"""
    if "_tq_subs" in st.session_state and st.session_state._tq_subs is not None:
        subs = st.session_state._tq_subs
        # 检查是否已订阅所有需要的合约
        cached_iids = set(subs.keys())
        needed_iids = {inst["instrument_id"] for inst in instruments}
        if needed_iids <= cached_iids:
            return subs

    # 需要新建订阅
    subs = {}
    for inst in instruments:
        iid = inst["instrument_id"]
        ex = inst["exchange"]
        sym = _to_tq_symbol(iid, ex)
        subs[iid] = {
            "sym": sym,
            "quote": api.get_quote(sym),
            "k1m": api.get_kline_serial(sym, 60, data_length=120),
            "k5m": api.get_kline_serial(sym, 300, data_length=60),
            "k1d": api.get_kline_serial(sym, 86400, data_length=30),
        }

    # 首次订阅后等待数据到达
    api.wait_update(deadline=time.time() + 15)
    st.session_state._tq_subs = subs
    return subs


def fetch_data_from_tq(instruments):
    """从天勤量化获取所有合约数据, 返回格式与 one_shot_fetch 兼容"""
    if not instruments:
        return None

    api, err = _get_tq_api()
    if err:
        return None

    try:
        # 使用缓存的订阅 (避免每次重新订阅)
        tq_subs = _get_tq_subscriptions(api, instruments)

        # 后续刷新只需短暂等待更新
        api.wait_update(deadline=time.time() + 3)

        # 提取数据
        data = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            sub = tq_subs.get(iid)
            if not sub:
                continue

            q = sub["quote"]
            import math as _math
            def _safe(v):
                if v is None: return 0
                try:
                    fv = float(v)
                    return 0 if _math.isnan(fv) else fv
                except: return 0

            tick = {
                "instrument_id": iid,
                "last_price": _safe(q.last_price),
                "open": _safe(q.open),
                "high": _safe(q.highest),
                "low": _safe(q.lowest),
                "high_price": _safe(q.highest),
                "low_price": _safe(q.lowest),
                "prev_settlement": _safe(q.pre_settlement),
                "pre_settlement_price": _safe(q.pre_settlement),
                "volume": int(_safe(q.volume)),
                "open_interest": int(_safe(q.open_interest)),
                "bid_price1": _safe(q.bid_price1),
                "ask_price1": _safe(q.ask_price1),
                "upper_limit": _safe(q.upper_limit),
                "lower_limit": _safe(q.lower_limit),
                "datetime": str(q.datetime) if q.datetime else "",
            }

            def _klines_to_list(kdf):
                result = []
                try:
                    from datetime import datetime as _dt
                    for _, row in kdf.iterrows():
                        t = row.get("datetime", 0)
                        if isinstance(t, (int, float)) and not _math.isnan(t) and t > 0:
                            try:
                                ts = t / 1e9 if t > 1e15 else t
                                time_str = _dt.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                            except:
                                time_str = str(t)
                        else:
                            continue
                        o = _safe(row.get("open", 0))
                        h = _safe(row.get("high", 0))
                        l = _safe(row.get("low", 0))
                        c = _safe(row.get("close", 0))
                        if o == 0 and h == 0:
                            continue
                        result.append({
                            "time": time_str,
                            "open": o, "high": h, "low": l, "close": c,
                            "volume": int(_safe(row.get("volume", 0))),
                            "open_interest": int(_safe(row.get("open_interest", 0))),
                        })
                except Exception:
                    pass
                return result

            data[iid] = {
                "inst": inst,
                "tick": tick,
                "candles_1m": _klines_to_list(sub["k1m"]),
                "candles_5m": _klines_to_list(sub["k5m"]),
                "candles_1d": _klines_to_list(sub["k1d"]),
            }

        return data if data else None

    except Exception as e:
        print(f"[TQ] fetch_data_from_tq error: {e}")
        return None


def tq_realtime(instruments, cache_sec=1):
    """天勤实时行情"""
    now = time.time()
    if "_tq_rt_cache" not in st.session_state:
        st.session_state._tq_rt_cache = {"ts": 0, "data": {}}
    cache = st.session_state._tq_rt_cache
    if now - cache["ts"] < cache_sec and cache["data"]:
        return cache["data"]

    api, err = _get_tq_api()
    if err:
        return cache["data"]

    try:
        # 复用已缓存的订阅, 避免重复 get_quote
        subs = _get_tq_subscriptions(api, instruments)

        # 短暂等待最新数据
        api.wait_update(deadline=time.time() + 2)

        import math as _math
        result = {}
        for inst in instruments:
            iid = inst["instrument_id"]
            sub = subs.get(iid)
            if sub:
                q = sub["quote"]
                lp = float(q.last_price) if q.last_price == q.last_price else 0
                if lp > 0:
                    result[iid] = {
                        "last_price": lp,
                        "open": float(q.open) if q.open == q.open else 0,
                        "high": float(q.highest) if q.highest == q.highest else 0,
                        "low": float(q.lowest) if q.lowest == q.lowest else 0,
                        "bid": float(q.bid_price1) if q.bid_price1 == q.bid_price1 else 0,
                        "ask": float(q.ask_price1) if q.ask_price1 == q.ask_price1 else 0,
                        "volume": int(q.volume) if q.volume == q.volume else 0,
                        "hold": int(q.open_interest) if q.open_interest == q.open_interest else 0,
                        "name": getattr(q, "instrument_name", ""),
                    }
        cache["ts"] = now
        cache["data"] = result
        return result
    except Exception:
        return cache["data"]



def save_state():
    """将关键状态持久化到 JSON 文件，下次启动自动恢复"""
    try:
        data = {
            "instruments": st.session_state.instruments,
            "positions": st.session_state.positions,
            "trade_log": st.session_state.trade_log[-100:],  # 只保留最近100条
            "auto_trading": st.session_state.get("auto_trading", False),
            "signal_threshold": st.session_state.get("signal_threshold", 50),
            "auto_trade_timeframe": st.session_state.get("auto_trade_timeframe", "1m"),
            "stop_loss_pct": st.session_state.get("stop_loss_pct", 2.0),
            "take_profit_pct": st.session_state.get("take_profit_pct", 4.0),
            "trade_volume": st.session_state.get("trade_volume", 1),
            "reversal_threshold": st.session_state.get("reversal_threshold", 30),
            "update_url": st.session_state.get("update_url", ""),
            "tq_user": st.session_state.get("tq_user", ""),
            "tq_pass": st.session_state.get("tq_pass", ""),
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_state():
    """从 JSON 文件恢复状态，返回 dict 或 None"""
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def check_for_update(update_url):
    """检查更新，返回 (has_update, remote_version, download_url) 或 (False, None, None)"""
    if not update_url:
        return False, None, None
    try:
        # 支持 HTTP URL 或本地/网络文件路径
        if update_url.startswith("http://") or update_url.startswith("https://"):
            resp = requests.get(update_url, timeout=10)
            resp.raise_for_status()
            info = resp.json()
        else:
            # 本地或网络路径 (如 \\server\share\update.json 或 C:\share\update.json)
            with open(update_url, "r", encoding="utf-8") as f:
                info = json.load(f)

        remote_ver = info.get("version", "0.0.0")
        download_url = info.get("download_url", "")

        # 简单版本比较
        def ver_tuple(v):
            try:
                return tuple(int(x) for x in v.split("."))
            except:
                return (0, 0, 0)

        has_update = ver_tuple(remote_ver) > ver_tuple(DASHBOARD_VERSION)
        return has_update, remote_ver, download_url
    except Exception as e:
        return False, None, str(e)


def download_update(download_url):
    """下载更新文件，返回 (success, message)"""
    if not download_url:
        return False, "无下载地址"
    try:
        dashboard_path = os.path.abspath(__file__)
        backup_path = dashboard_path + ".backup"

        # 下载新文件
        if download_url.startswith("http://") or download_url.startswith("https://"):
            resp = requests.get(download_url, timeout=60)
            resp.raise_for_status()
            new_content = resp.content
        else:
            # 本地或网络路径
            with open(download_url, "rb") as f:
                new_content = f.read()

        # 备份当前文件
        import shutil
        shutil.copy2(dashboard_path, backup_path)

        # 写入新文件
        with open(dashboard_path, "wb") as f:
            f.write(new_content)

        return True, "更新成功，请重启面板生效"
    except Exception as e:
        return False, f"更新失败: {e}"


DEFAULT_INSTRUMENTS = [
    {"exchange": "CZCE", "instrument_id": "FG609", "label": "玻璃09", "size": 20, "price_tick": 1.0, "group": "玻璃"},
    {"exchange": "CZCE", "instrument_id": "FG610", "label": "玻璃10", "size": 20, "price_tick": 1.0, "group": "玻璃"},
    {"exchange": "CZCE", "instrument_id": "SA609", "label": "纯碱09", "size": 20, "price_tick": 1.0, "group": "纯碱"},
    {"exchange": "CZCE", "instrument_id": "SA610", "label": "纯碱10", "size": 20, "price_tick": 1.0, "group": "纯碱"},
    {"exchange": "SHFE", "instrument_id": "rb2609", "label": "螺纹09", "size": 10, "price_tick": 1.0, "group": "螺纹"},
    {"exchange": "SHFE", "instrument_id": "rb2610", "label": "螺纹10", "size": 10, "price_tick": 1.0, "group": "螺纹"},
    {"exchange": "SHFE", "instrument_id": "ag2609", "label": "白银09", "size": 15, "price_tick": 1.0, "group": "白银"},
    {"exchange": "SHFE", "instrument_id": "ag2610", "label": "白银10", "size": 15, "price_tick": 1.0, "group": "白银"},
]

PRODUCT_REFERENCE = {
    "SHFE (上期所)": {
        "rb": ("螺纹钢", 10, 1.0), "ag": ("白银", 15, 1.0), "au": ("黄金", 1000, 0.02),
        "cu": ("铜", 5, 10.0), "al": ("铝", 5, 5.0), "zn": ("锌", 5, 5.0),
        "ni": ("镍", 1, 10.0), "ru": ("橡胶", 10, 5.0), "fu": ("燃油", 10, 1.0),
        "bu": ("沥青", 10, 2.0), "hc": ("热卷", 10, 1.0), "ss": ("不锈钢", 5, 5.0),
    },
    "CZCE (郑州)": {
        "FG": ("玻璃", 20, 1.0), "SA": ("纯碱", 20, 1.0), "SR": ("白糖", 10, 1.0),
        "CF": ("棉花", 5, 5.0), "TA": ("PTA", 5, 2.0), "MA": ("甲醇", 10, 1.0),
        "RM": ("菜粕", 10, 1.0), "OI": ("菜油", 10, 1.0), "AP": ("苹果", 10, 1.0),
        "SF": ("硅铁", 5, 2.0), "SM": ("锰硅", 5, 2.0), "UR": ("尿素", 20, 1.0),
    },
    "DCE (大连)": {
        "v": ("PVC", 5, 5.0), "m": ("豆粕", 10, 1.0), "y": ("豆油", 10, 2.0),
        "p": ("棕榈油", 10, 2.0), "a": ("豆一", 10, 1.0), "i": ("铁矿", 100, 0.5),
        "j": ("焦炭", 100, 0.5), "jm": ("焦煤", 60, 0.5), "l": ("塑料", 5, 5.0),
        "pp": ("聚丙烯", 5, 1.0), "eg": ("乙二醇", 10, 1.0), "eb": ("苯乙烯", 5, 1.0),
        "c": ("玉米", 10, 1.0), "pg": ("液化气", 20, 1.0),
    },
    "CFFEX (中金所)": {
        "IF": ("沪深300", 300, 0.2), "IC": ("中证500", 200, 0.2),
        "IM": ("中证1000", 200, 0.2), "IH": ("上证50", 300, 0.2),
        "T": ("十年国债", 10000, 0.005), "TF": ("五年国债", 10000, 0.005),
    },
    "INE (能源中心)": {
        "sc": ("原油", 1000, 0.1), "nr": ("20号胶", 10, 5.0),
        "lu": ("低硫燃油", 10, 1.0), "ec": ("集运指数", 50, 0.1),
    },
}

EXCHANGE_MONTH_DIGITS = {"CZCE": 3, "SHFE": 4, "DCE": 4, "CFFEX": 4, "INE": 4, "GFEX": 4}


# ============================================================================
# 一次性 MCP 连接+取数据 (跟测试成功的方式完全一样)
# ============================================================================

def one_shot_fetch(instruments):
    """一次性: 连接MCP → 握手 → 订阅 → 取所有数据 → 关闭. 返回 data dict 或 None"""
    if not instruments:
        return None

    # 1. SSE 连接
    try:
        resp = requests.get(MCP_SSE_URL, stream=True, timeout=10,
                            headers={"Accept": "text/event-stream"})
        resp.raise_for_status()
    except Exception:
        return None

    state = {"endpoint": None}
    responses = {}
    lock = threading.Lock()

    def listen():
        et = "message"; dl = []
        try:
            for line in resp.iter_lines(decode_unicode=True):
                if line.startswith("event:"):
                    et = line[6:].strip()
                elif line.startswith("data:"):
                    dl.append(line[5:].strip())
                elif line == "":
                    if dl:
                        raw = "\n".join(dl)
                        if et == "endpoint":
                            state["endpoint"] = raw
                        elif et == "message":
                            try:
                                d = json.loads(raw)
                                if "id" in d:
                                    with lock:
                                        responses[d["id"]] = d
                            except Exception:
                                pass
                        et = "message"; dl = []
        except Exception:
            pass

    t = threading.Thread(target=listen, daemon=True)
    t.start()

    deadline = time.time() + 5
    while time.time() < deadline and not state["endpoint"]:
        time.sleep(0.05)
    if not state["endpoint"]:
        resp.close()
        return None

    url = f"http://127.0.0.1:3000{state['endpoint']}" if state["endpoint"].startswith("/") else state["endpoint"]
    rid = [0]

    def post(name, args, tmo=10):
        rid[0] += 1
        my_rid = rid[0]
        try:
            requests.post(url, json={"jsonrpc": "2.0", "id": my_rid, "method": "tools/call",
                                     "params": {"name": name, "arguments": args}}, timeout=tmo)
        except Exception:
            return None
        dl = time.time() + tmo
        while time.time() < dl:
            with lock:
                if my_rid in responses:
                    r = responses.pop(my_rid)
                    if "error" in r:
                        return None
                    return r.get("result", {})
            time.sleep(0.05)
        return None

    def parse(result):
        if not result: return None
        c = result.get("content", [])
        if not c: return None
        # Multiple content items = list of objects (e.g. fetch_candles returns one content per candle)
        if len(c) > 1:
            items = []
            for item in c:
                txt = item.get("text", "") if isinstance(item, dict) else str(item)
                try: items.append(json.loads(txt))
                except: pass
            return items if items else None
        # Single content item
        txt = c[0].get("text", "") if isinstance(c, list) else str(c)
        txt = txt.strip()
        if not txt: return None
        try: return json.loads(txt)
        except: pass
        # Handle newline-separated JSON objects in single text
        if txt.startswith("{"):
            items = []
            for line in txt.split("\n"):
                line = line.strip()
                if not line: continue
                try: items.append(json.loads(line))
                except: pass
            if items: return items
        return None

    def normalize_candles(data):
        """Ensure candle data uses 'time' field (MCP returns 'datetime')"""
        if not isinstance(data, list): return []
        for c in data:
            if "datetime" in c and "time" not in c:
                c["time"] = c["datetime"]
        return data

    # 2. MCP 握手
    rid[0] += 1
    init_rid = rid[0]
    try:
        requests.post(url, json={"jsonrpc": "2.0", "id": init_rid, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                       "clientInfo": {"name": "Dashboard", "version": "1.0"}}}, timeout=5)
    except Exception:
        resp.close()
        return None
    dl = time.time() + 5
    while time.time() < dl:
        with lock:
            if init_rid in responses:
                responses.pop(init_rid); break
        time.sleep(0.05)
    try:
        requests.post(url, json={"jsonrpc": "2.0", "method": "notifications/initialized"}, timeout=5)
    except Exception:
        pass

    # 3. 订阅所有合约 (并行发送，减少等待)
    for inst in instruments:
        post("start_quote_feed", {"exchange": inst["exchange"], "instrument_id": inst["instrument_id"]})
        time.sleep(0.1)
    time.sleep(1.5)

    # 4. 并行取数据 (tick + 1分钟K线, 去掉5分钟K线减少请求)
    data = {}

    def fetch_instrument(inst):
        iid = inst["instrument_id"]
        ex = inst["exchange"]
        tick = parse(post("fetch_tick", {"exchange": ex, "instrument_id": iid}))
        candles_1m = parse(post("fetch_candles", {"exchange": ex, "instrument_id": iid, "style": "M1", "count": -120, "simply": False}))
        candles_5m = parse(post("fetch_candles", {"exchange": ex, "instrument_id": iid, "style": "M5", "count": -60, "simply": False}))
        candles_1d = parse(post("fetch_candles", {"exchange": ex, "instrument_id": iid, "style": "D1", "count": -30, "simply": False}))
        return iid, {
            "inst": inst, "tick": tick,
            "candles_1m": normalize_candles(candles_1m if isinstance(candles_1m, list) else None),
            "candles_5m": normalize_candles(candles_5m if isinstance(candles_5m, list) else None),
            "candles_1d": normalize_candles(candles_1d if isinstance(candles_1d, list) else None),
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(fetch_instrument, inst) for inst in instruments]
        for f in as_completed(futures):
            try:
                iid, d = f.result(timeout=15)
                data[iid] = d
            except Exception:
                pass

    # 5. 关闭
    resp.close()
    return data


def send_trade(name, args):
    """发送交易指令 (一次性连接)"""
    result = None
    try:
        resp = requests.get(MCP_SSE_URL, stream=True, timeout=8, headers={"Accept": "text/event-stream"})
        state = {"ep": None}
        responses = {}
        lock = threading.Lock()
        def listen():
            et = "message"; dl = []
            for line in resp.iter_lines(decode_unicode=True):
                if line.startswith("event:"): et = line[6:].strip()
                elif line.startswith("data:"): dl.append(line[5:].strip())
                elif line == "":
                    if dl:
                        raw = "\n".join(dl)
                        if et == "endpoint": state["ep"] = raw
                        elif et == "message":
                            try:
                                d = json.loads(raw)
                                if "id" in d:
                                    with lock: responses[d["id"]] = d
                            except: pass
                        et = "message"; dl = []
        t = threading.Thread(target=listen, daemon=True)
        t.start()
        for _ in range(50):
            if state["ep"]: break
            time.sleep(0.1)
        if not state["ep"]: resp.close(); return {"ok": False, "error": "连接失败"}
        u = f"http://127.0.0.1:3000{state['ep']}" if state["ep"].startswith("/") else state["ep"]
        rid = [1]
        requests.post(u, json={"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"T","version":"1.0"}}}, timeout=5)
        for _ in range(50):
            with lock:
                if 1 in responses: responses.pop(1); break
            time.sleep(0.1)
        requests.post(u, json={"jsonrpc":"2.0","method":"notifications/initialized"}, timeout=5)
        rid[0] += 1
        requests.post(u, json={"jsonrpc":"2.0","id":rid[0],"method":"tools/call","params":{"name":name,"arguments":args}}, timeout=10)
        for _ in range(100):
            with lock:
                if rid[0] in responses: result = responses.pop(rid[0]); break
            time.sleep(0.1)
        resp.close()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if result:
        c = result.get("result", {}).get("content", [])
        if c:
            try: return {"ok": True, "data": json.loads(c[0]["text"])}
            except: return {"ok": True, "data": c}
    return {"ok": False, "error": "无响应"}


# ============================================================================
# 分析函数
# ============================================================================

def calc_delta(candles):
    deltas = []
    for c in candles:
        o, cl, vol, t = c.get("open",0), c.get("close",0), c.get("volume",0), c.get("time","")
        if vol <= 0: continue
        if cl >= o: bv = int(vol*0.6); sv = vol-bv
        else: sv = int(vol*0.6); bv = vol-sv
        deltas.append({"time":t,"delta":bv-sv,"buy_vol":bv,"sell_vol":sv,"volume":vol,"close":cl,"open":o})
    return deltas

def calc_volume_profile(candles):
    p = {}
    for c in candles:
        h,l,cl,v = c.get("high",0),c.get("low",0),c.get("close",0),c.get("volume",0)
        if h<=0 or v<=0: continue
        tp = round((h+l+cl)/3, 0); p[tp] = p.get(tp,0)+v
    return p

def analyze_vpo(candles):
    if not candles or len(candles)<5: return {"state":"数据不足","score":0,"vol_ratio":0,"price_change":0}
    recent=candles[-5:]; older=candles[-20:-5] if len(candles)>=20 else candles[:-5]
    if not older: older=candles[:max(1,len(candles)//2)]
    avo=sum(c.get("volume",0) for c in older)/max(len(older),1)
    avr=sum(c.get("volume",0) for c in recent)/max(len(recent),1)
    vr=avr/max(avo,1); pf=older[0].get("close",0) if older else recent[0].get("close",0)
    pc=recent[-1].get("close",0)-pf; vu=vr>=1.5; pu=pc>0
    if vu and pu: s,sc="放量上涨 - 多头进攻",65
    elif vu and not pu: s,sc="放量下跌 - 空头进攻",-65
    elif not vu and pu: s,sc="缩量上涨 - 动能不足",-15
    else: s,sc="缩量下跌 - 空头衰竭",15
    return {"state":s,"score":sc,"vol_ratio":round(vr,2),"price_change":round(pc,1)}

def analyze_momentum(deltas):
    if not deltas or len(deltas)<5: return {"trend":"数据不足","score":0,"ratio":1.0,"buy_energy":0,"sell_energy":0}
    short=deltas[-5:]; long_=deltas[-20:] if len(deltas)>=20 else deltas
    ss=sum(d["delta"] for d in short); ls=sum(d["delta"] for d in long_)
    be=sum(max(0,d["delta"]) for d in short); se=sum(abs(min(0,d["delta"])) for d in short)
    r=be/max(se,1)
    if ss>0 and ls>0: tr="加速上涨" if ss>ls/max(len(long_),1)*5 else "减速上涨"
    elif ss<0 and ls<0: tr="加速下跌" if ss<ls/max(len(long_),1)*5 else "减速下跌"
    elif ss>0 and ls<=0: tr="短期反弹"
    elif ss<0 and ls>=0: tr="短期回调"
    else: tr="震荡"
    sc=max(-100,min(100,ss/max(len(short),1)*2))
    return {"trend":tr,"score":round(sc,1),"ratio":round(r,2),"buy_energy":be,"sell_energy":se}

def find_key_levels(profile, cp):
    if not profile: return {"support":None,"resistance":None,"poc":None}
    sl=sorted(profile.items(),key=lambda x:x[1],reverse=True)
    poc=sl[0] if sl else None
    sp=sorted([(p,v) for p,v in sl if p<cp],key=lambda x:abs(x[0]-cp))
    rp=sorted([(p,v) for p,v in sl if p>cp],key=lambda x:abs(x[0]-cp))
    return {"poc":{"price":poc[0],"volume":poc[1]} if poc else None,
            "support":{"price":sp[0][0],"volume":sp[0][1]} if sp else None,
            "resistance":{"price":rp[0][0],"volume":rp[0][1]} if rp else None}


def calc_poc_migration(candles, window=10):
    """计算 POC 迁移轨迹 - 追踪主力成本移动方向"""
    if not candles or len(candles) < window:
        return {"migration": [], "direction": "数据不足", "trend": 0}

    migration = []
    for i in range(window, len(candles) + 1):
        segment = candles[i-window:i]
        profile = calc_volume_profile(segment)
        if profile:
            poc_price = max(profile.items(), key=lambda x: x[1])[0]
            migration.append({"index": i, "price": poc_price, "time": segment[-1].get("time", "")})

    if len(migration) < 2:
        return {"migration": migration, "direction": "数据不足", "trend": 0}

    # 计算 POC 移动方向和速度
    first_poc = migration[0]["price"]
    last_poc = migration[-1]["price"]
    diff = last_poc - first_poc
    pct_change = diff / first_poc * 100 if first_poc > 0 else 0

    if pct_change > 0.3:
        direction = "上移 ↑"
    elif pct_change < -0.3:
        direction = "下移 ↓"
    else:
        direction = "横盘 →"

    return {
        "migration": migration,
        "direction": direction,
        "trend": round(pct_change, 2),
        "first_poc": first_poc,
        "last_poc": last_poc,
    }


def calc_multi_timeframe_poc(data):
    """多周期 POC 计算 - 1分钟/5分钟/日线"""
    result = {"1m": None, "5m": None, "1d": None}

    for tf, key in [("1m", "candles_1m"), ("5m", "candles_5m"), ("1d", "candles_1d")]:
        candles = data.get(key, [])
        if candles and len(candles) >= 5:
            profile = calc_volume_profile(candles)
            if profile:
                poc = max(profile.items(), key=lambda x: x[1])
                result[tf] = {"price": poc[0], "volume": poc[1]}

    return result


def calc_historical_poc(candles_1d, days=5):
    """计算历史 POC - 昨日/前日/本周等关键价位"""
    if not candles_1d or len(candles_1d) < 2:
        return {"yesterday": None, "prev_days": []}

    result = {"yesterday": None, "prev_days": []}

    # 按日分组计算每日 POC
    daily_pocs = []
    for candle in candles_1d[-days-1:]:
        # 日线K线本身就是一个交易日，用OHLC估算POC
        h = candle.get("high", 0)
        l = candle.get("low", 0)
        c = candle.get("close", 0)
        v = candle.get("volume", 0)
        if h > 0 and l > 0 and v > 0:
            # 典型价格 = (H+L+C)/3 作为POC近似
            typical_price = (h + l + c) / 3
            daily_pocs.append({
                "date": candle.get("time", "")[:10],
                "poc": round(typical_price, 1),
                "volume": v,
                "high": h,
                "low": l,
            })

    if daily_pocs:
        result["yesterday"] = daily_pocs[-1] if daily_pocs else None
        result["prev_days"] = daily_pocs[:-1] if len(daily_pocs) > 1 else []

    return result


def execute_auto_trades(data, instruments, threshold, sl_pct, tp_pct, volume, timeframe="1m"):
    """根据综合信号自动执行交易 - 支持多周期"""
    # 根据周期选择K线数据
    tf_map = {"1m": "candles_1m", "5m": "candles_5m", "15m": "candles_15m", "1d": "candles_1d"}
    candles_key = tf_map.get(timeframe, "candles_1m")

    results = []
    for inst in instruments:
        iid = inst["instrument_id"]
        exchange = inst["exchange"]
        d = data.get(iid, {})
        candles = d.get(candles_key, [])
        tick = d.get("tick")
        if not candles or not tick or isinstance(tick, str):
            continue
        cp = tick.get("last_price", 0)
        if not cp or cp <= 0:
            continue

        # 计算综合信号 (与信号Tab一致)
        ds = calc_delta(candles)
        if len(ds) < 5:
            continue
        prof = calc_volume_profile(candles)
        lv = find_key_levels(prof, cp)
        of_val = max(-100, min(100, sum(dd["delta"] for dd in ds[-5:]) / max(len(ds[-5:]), 1) * 2))
        vpo = analyze_vpo(candles)
        vs = vpo["score"]
        kl = 0
        if lv["support"] and abs(cp - lv["support"]["price"]) / cp * 100 < 0.5:
            kl = 40
        if lv["resistance"] and abs(cp - lv["resistance"]["price"]) / cp * 100 < 0.5:
            kl = -40
        mom = analyze_momentum(ds)
        ms = mom["score"]
        total = max(-100, min(100, of_val * 0.3 + vs * 0.25 + kl * 0.2 + ms * 0.25))

        if abs(total) < threshold:
            continue

        direction = "buy" if total > 0 else "sell"

        # 先检查本地追踪的持仓 (即使MCP断开也能防止重复开仓)
        if iid in st.session_state.positions:
            continue

        # 再检查实际账户持仓 (双重保险)
        holding = send_trade("fetch_holding", {"instrument_id": iid})
        has_position = False
        if holding.get("ok") and holding.get("data"):
            hd = holding["data"]
            net = hd.get("net_position", 0) if isinstance(hd, dict) else 0
            if net != 0:
                has_position = True

        if has_position:
            # 实际有持仓但本地没追踪, 补充记录
            st.session_state.positions[iid] = {
                "direction": direction, "entry_price": cp, "volume": volume,
                "sl": 0, "tp": 0
            }
            continue

        # 开仓
        open_result = send_trade("submit_open", {
            "exchange": exchange,
            "instrument_id": iid,
            "volume": volume,
            "price": 0,
            "direction": direction,
            "market": True,
            "memo": f"Auto signal={total:+.1f}"
        })

        if not open_result.get("ok"):
            results.append({
                "instrument_id": iid, "action": "开仓失败",
                "direction": direction, "volume": 0, "price": cp,
                "stop_loss": 0, "take_profit": 0,
                "message": open_result.get("error", "unknown"),
                "time": datetime.now().strftime("%H:%M:%S")
            })
            continue

        # 设置止损止盈
        sl_price = round(cp * (1 - sl_pct / 100), 1) if direction == "buy" else round(cp * (1 + sl_pct / 100), 1)
        tp_price = round(cp * (1 + tp_pct / 100), 1) if direction == "buy" else round(cp * (1 - tp_pct / 100), 1)

        send_trade("set_stop_loss", {
            "exchange": exchange, "instrument_id": iid,
            "stop_price": sl_price, "volume": volume, "market": True
        })
        send_trade("set_take_profit", {
            "exchange": exchange, "instrument_id": iid,
            "take_price": tp_price, "volume": volume, "market": True
        })

        dir_label = "做多" if direction == "buy" else "做空"
        trade_record = {
            "instrument_id": iid, "action": f"自动{dir_label}",
            "direction": direction, "volume": volume, "price": cp,
            "stop_loss": sl_price, "take_profit": tp_price,
            "signal": total, "message": f"信号{total:+.1f}",
            "time": datetime.now().strftime("%H:%M:%S")
        }
        results.append(trade_record)
        st.session_state.trade_log.append(trade_record)
        # 记录持仓目标价用于后续止盈止损检查
        st.session_state.positions[iid] = {
            "direction": direction, "entry_price": cp, "volume": volume,
            "sl": sl_price, "tp": tp_price
        }

        # 写日志到MCP
        send_trade("write_log", {
            "level": "INFO",
            "message": f"AutoTrade {dir_label} {iid} {volume}手 @ {cp}, 信号={total:+.1f}, SL={sl_price}, TP={tp_price}"
        })

    return results


def check_positions_sl_tp(data, instruments, reversal_threshold=30, timeframe="1m"):
    """检查持仓: 固定止盈止损 + 信号反转平仓 - 支持多周期"""
    # 根据周期选择K线数据
    tf_map = {"1m": "candles_1m", "5m": "candles_5m", "15m": "candles_15m", "1d": "candles_1d"}
    candles_key = tf_map.get(timeframe, "candles_1m")

    results = []
    inst_map = {inst["instrument_id"]: inst for inst in instruments}

    for iid, pos in list(st.session_state.positions.items()):
        d = data.get(iid, {})
        tick = d.get("tick")
        if not tick or not isinstance(tick, dict):
            continue
        cp = tick.get("last_price", 0)
        if not cp:
            continue

        direction = pos["direction"]
        sl = pos["sl"]
        tp = pos["tp"]
        entry = pos["entry_price"]
        vol = pos["volume"]
        triggered = None
        trigger_detail = ""

        # === 1. 固定止盈止损 (安全网) ===
        if direction == "buy":
            if cp <= sl:
                triggered = "止损"
                trigger_detail = f"现价{cp:.0f}<=止损{sl:.0f}"
            elif cp >= tp:
                triggered = "止盈"
                trigger_detail = f"现价{cp:.0f}>=止盈{tp:.0f}"
        else:
            if cp >= sl:
                triggered = "止损"
                trigger_detail = f"现价{cp:.0f}>=止损{sl:.0f}"
            elif cp <= tp:
                triggered = "止盈"
                trigger_detail = f"现价{cp:.0f}<=止盈{tp:.0f}"

        # === 2. 信号反转平仓 (主动出场) ===
        if not triggered:
            candles = d.get(candles_key, [])
            if candles and len(candles) >= 5:
                ds = calc_delta(candles)
                prof = calc_volume_profile(candles)
                lv = find_key_levels(prof, cp)
                of_val = max(-100, min(100, sum(dd["delta"] for dd in ds[-5:]) / max(len(ds[-5:]), 1) * 2))
                vpo = analyze_vpo(candles)
                vs = vpo["score"]
                kl = 0
                if lv["support"] and abs(cp - lv["support"]["price"]) / cp * 100 < 0.5:
                    kl = 40
                if lv["resistance"] and abs(cp - lv["resistance"]["price"]) / cp * 100 < 0.5:
                    kl = -40
                mom = analyze_momentum(ds)
                ms = mom["score"]
                signal = max(-100, min(100, of_val * 0.3 + vs * 0.25 + kl * 0.2 + ms * 0.25))

                # 做多持仓，信号反转做空 → 平仓
                if direction == "buy" and signal < -reversal_threshold:
                    triggered = "信号反转"
                    trigger_detail = f"多仓遇空信号{signal:+.1f} (阈值-{reversal_threshold})"
                # 做空持仓，信号反转做多 → 平仓
                elif direction == "sell" and signal > reversal_threshold:
                    triggered = "信号反转"
                    trigger_detail = f"空仓遇多信号{signal:+.1f} (阈值+{reversal_threshold})"

        if not triggered:
            continue

        # === 触发平仓 ===
        inst = inst_map.get(iid, {})
        exchange = inst.get("exchange", "")
        close_dir = "sell" if direction == "buy" else "buy"

        close_result = send_trade("submit_close", {
            "exchange": exchange,
            "instrument_id": iid,
            "volume": vol,
            "price": 0,
            "direction": close_dir,
            "market": True,
            "memo": f"Auto {triggered}"
        })

        pnl = (cp - entry) * vol if direction == "buy" else (entry - cp) * vol
        pnl_label = "+" if pnl > 0 else ""
        record = {
            "instrument_id": iid,
            "action": f"{triggered}平仓",
            "direction": direction,
            "volume": vol,
            "price": cp,
            "stop_loss": sl,
            "take_profit": tp,
            "message": f"{trigger_detail}, 入场{entry}, 盈亏{pnl_label}{pnl:.0f}",
            "time": datetime.now().strftime("%H:%M:%S")
        }
        results.append(record)
        st.session_state.trade_log.append(record)

        del st.session_state.positions[iid]

        send_trade("write_log", {
            "level": "INFO",
            "message": f"AutoClose {triggered} {iid} {vol}手 @ {cp}, {trigger_detail}, 盈亏{pnl_label}{pnl:.0f}"
        })

    return results


# ============================================================================
# 天勤实时行情 (复用上面的 tq_realtime 函数)
# ============================================================================


# ============================================================================
# 机构持仓排名 (akshare, 每日收盘后更新, session_state缓存)
# ============================================================================

EXCHANGE_AK_FUNC = {
    "SHFE": "get_shfe_rank_table",
    "DCE": "get_dce_rank_table",
    "CZCE": "get_rank_table_czce",
}


def _normalize_contract_for_rank(exchange, instrument_id):
    """将合约代码转为排名数据中的key格式 (返回小写)"""
    iid = instrument_id
    if exchange == "CZCE":
        import re as _re
        m = _re.match(r"^([A-Za-z]+)(\d{3})$", iid)
        if m:
            # CZCE排名数据key格式: 品种+3位月份, 如 "UR701"; 也有纯品种如 "AP"
            return f"{m.group(1)}{m.group(2)}".lower()
    return iid.lower()


def _safe_int(val, default=0):
    """安全转int, 处理逗号格式 (如CZCE的 '102,050') 和浮点格式 (如SHFE的 25316.0)"""
    try:
        if val is None or str(val) == "nan":
            return default
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return default


def fetch_position_rank(instruments, top_n=5):
    """获取机构多空持仓排名, 使用session_state缓存避免重复请求.
    返回 {instrument_id: {longs: [...], shorts: [...], date: str}}
    注意: DCE大商所网站经常超时/反爬, 默认跳过.
    """
    # 初始化session_state缓存
    if "_rank_cache" not in st.session_state:
        st.session_state._rank_cache = {"fetch_time": 0, "data": {}, "exchanges": set(), "failed": {}}
    # 兼容旧缓存 (可能没有"failed"键)
    cache = st.session_state._rank_cache
    if "failed" not in cache:
        cache["failed"] = {}
    if "data" not in cache:
        cache["data"] = {}
    if "exchanges" not in cache:
        cache["exchanges"] = set()
    now = time.time()
    CACHE_TTL = 3600  # 缓存1小时 (持仓排名是日频数据)
    FAIL_TTL = 7200   # 失败记录缓存2小时

    # 确定需要哪些交易所
    exchanges_needed = set()
    for inst in instruments:
        ex = inst["exchange"]
        if ex in EXCHANGE_AK_FUNC:
            exchanges_needed.add(ex)

    # 跳过DCE (大商所网站经常超时/返回412)
    exchanges_needed.discard("DCE")

    # 跳过最近失败的交易所
    to_fetch = set()
    for ex in exchanges_needed:
        fail_time = cache["failed"].get(ex, 0)
        if now - fail_time < FAIL_TTL:
            continue  # 最近失败过, 跳过
        to_fetch.add(ex)

    # 判断是否需要刷新
    new_ex = to_fetch - cache["exchanges"]
    need_refresh = (now - cache["fetch_time"] > CACHE_TTL) or bool(new_ex)

    if need_refresh and to_fetch:
        import akshare as ak
        import warnings
        from datetime import timedelta

        d = datetime.now()
        # 只查最近3天, 减少非交易日警告
        query_dates = [(d - timedelta(days=i)).strftime("%Y%m%d") for i in range(1, 4)]

        fetch_set = to_fetch if now - cache["fetch_time"] > CACHE_TTL else new_ex

        def _fetch_one_exchange(ex):
            """获取单个交易所的排名数据 (在子线程中运行, 有超时保护)"""
            func_name = EXCHANGE_AK_FUNC.get(ex)
            if not func_name:
                return ex, None
            func = getattr(ak, func_name)
            for qd in query_dates:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        rank_data = func(date=qd)
                except Exception:
                    continue
                if not rank_data or not isinstance(rank_data, dict):
                    continue
                return ex, (rank_data, qd)
            return ex, None

        # 每个交易所最多10秒, 总超时15秒
        done_exchanges = set()
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures_map = {pool.submit(_fetch_one_exchange, ex): ex for ex in fetch_set}
            try:
                for future in as_completed(futures_map, timeout=15):
                    try:
                        ex, result_tuple = future.result(timeout=10)
                    except Exception:
                        # 单个future异常, 只标记该交易所
                        done_ex = futures_map.get(future)
                        if done_ex:
                            cache["failed"][done_ex] = now
                        continue
                    done_exchanges.add(ex)
                    if result_tuple is None:
                        cache["failed"][ex] = now
                        continue
                    rank_data, qd = result_tuple
                    for contract_key, df in rank_data.items():
                        if df is None or len(df) == 0:
                            continue
                        longs, shorts = [], []
                        for _, row in df.head(top_n).iterrows():
                            lpn = row.get("long_party_name")
                            spn = row.get("short_party_name")
                            if lpn and str(lpn) != "nan":
                                longs.append({
                                    "name": str(lpn),
                                    "hold": _safe_int(row.get("long_open_interest")),
                                    "chg": _safe_int(row.get("long_open_interest_chg")),
                                })
                            if spn and str(spn) != "nan":
                                shorts.append({
                                    "name": str(spn),
                                    "hold": _safe_int(row.get("short_open_interest")),
                                    "chg": _safe_int(row.get("short_open_interest_chg")),
                                })
                        if longs or shorts:
                            cache["data"][f"{ex}_{contract_key.lower()}"] = {
                                "longs": longs, "shorts": shorts, "date": qd
                            }
                    cache["exchanges"].add(ex)
                    cache["failed"].pop(ex, None)  # 成功则清除失败记录
            except Exception:
                # 总超时: 只标记未完成的交易所
                for fut, ex in futures_map.items():
                    if ex not in done_exchanges:
                        cache["failed"][ex] = now
        cache["fetch_time"] = now

    # 从缓存中提取请求的合约
    result = {}
    for inst in instruments:
        exchange = inst["exchange"]
        iid = inst["instrument_id"]
        norm_id = _normalize_contract_for_rank(exchange, iid)
        key = f"{exchange}_{norm_id}"
        if key in cache["data"]:
            result[iid] = cache["data"][key]
        else:
            # fallback: 尝试纯品种名 (如 "czce_fg" for FG609)
            import re as _re3
            m = _re3.match(r"^([A-Za-z]+)", norm_id)
            if m:
                variety_key = f"{exchange}_{m.group(1).lower()}"
                if variety_key in cache["data"]:
                    result[iid] = cache["data"][variety_key]
    return result


# ============================================================================
# 页面
# ============================================================================

st.set_page_config(page_title="订单流量化分析", page_icon="📊", layout="wide", initial_sidebar_state="expanded")
st.markdown("""<style>
    .stMetric>div{background:rgba(30,30,40,0.5);border-radius:8px;padding:8px 12px}
    div[data-testid="stHorizontalBlock"]{gap:8px}
    .block-container{padding-top:1rem}
    .stTabs [data-baseweb="tab-list"]{gap:8px}
    .stTabs [data-baseweb="tab"]{padding:4px 16px}
</style>""", unsafe_allow_html=True)


# ============================================================================
# Session State
# ============================================================================

if "_init" not in st.session_state:
    st.session_state._init = True
    saved = load_state()
    if saved:
        st.session_state.instruments = saved.get("instruments", list(DEFAULT_INSTRUMENTS))
        st.session_state.positions = saved.get("positions", {})
        st.session_state.trade_log = saved.get("trade_log", [])
        st.session_state.auto_trading = saved.get("auto_trading", False)
        st.session_state.signal_threshold = saved.get("signal_threshold", 50)
        st.session_state.auto_trade_timeframe = saved.get("auto_trade_timeframe", "1m")
        st.session_state.stop_loss_pct = saved.get("stop_loss_pct", 2.0)
        st.session_state.take_profit_pct = saved.get("take_profit_pct", 4.0)
        st.session_state.trade_volume = saved.get("trade_volume", 1)
        st.session_state.reversal_threshold = saved.get("reversal_threshold", 30)
        st.session_state.update_url = saved.get("update_url", "")
        st.session_state.tq_user = saved.get("tq_user", "")
        st.session_state.tq_pass = saved.get("tq_pass", "")
    else:
        st.session_state.instruments = list(DEFAULT_INSTRUMENTS)
        st.session_state.positions = {}
        st.session_state.trade_log = []
    st.session_state.prev_ticks = {}
    st.session_state.connected = False
    st.session_state.data = {}
    st.session_state.start_time = None
    st.session_state.capital_info = None
    st.session_state.holdings_info = {}


# ============================================================================
# 侧边栏
# ============================================================================

with st.sidebar:
    st.markdown("## 合约管理")
    if st.session_state.instruments:
        st.markdown(f"**监控: {len(st.session_state.instruments)} 个合约**")
        rm = []
        for idx, inst in enumerate(st.session_state.instruments):
            c1,c2 = st.columns([4,1])
            with c1: st.markdown(f"`{inst['instrument_id']}` {inst['label']}")
            with c2:
                if st.button("X", key=f"rm_{idx}"): rm.append(idx)
        if rm:
            for i in sorted(rm, reverse=True): st.session_state.instruments.pop(i)
            save_state(); st.rerun()
    else:
        st.info("暂无合约")
    st.markdown("---")
    st.markdown("**添加合约**")
    el = list(PRODUCT_REFERENCE.keys())
    ed = st.selectbox("交易所", el, key="ae")
    ec = ed.split(" ")[0]
    prods = PRODUCT_REFERENCE.get(ed, {})
    if prods:
        po = [f"{c} - {i[0]}" for c,i in prods.items()]
        pd_ = st.selectbox("品种", po, key="ap")
        pc = pd_.split(" - ")[0]; pi = prods.get(pc, ("",10,1.0))
        st.caption(f"乘数:{pi[1]} 变动:{pi[2]}")
    else:
        pc = st.text_input("品种代码", key="apc"); pi = ("自定义",10,1.0)
    md = EXCHANGE_MONTH_DIGITS.get(ec,4)
    mi = st.text_input(f"月份 ({'3位如609' if md==3 else '4位如2609'})", placeholder="609" if md==3 else "2609", key="am")
    if st.button("添加", type="primary", key="ba"):
        if not pc: st.warning("选品种")
        elif not mi: st.warning("输月份")
        else:
            iid = f"{pc}{mi}"
            if iid in {i["instrument_id"] for i in st.session_state.instruments}: st.warning("已存在")
            else:
                st.session_state.instruments.append({"exchange":ec,"instrument_id":iid,"label":f"{pi[0]}{mi}","size":pi[1],"price_tick":pi[2],"group":pi[0]})
                save_state(); st.rerun()
    st.markdown("---")
    with st.expander("批量添加"):
        be2 = st.selectbox("交易所", el, key="be2"); bc2=be2.split(" ")[0]
        bp = PRODUCT_REFERENCE.get(be2,{})
        if bp:
            bpo = [f"{c} - {i[0]}" for c,i in bp.items()]
            bs = st.multiselect("品种", bpo, key="bs")
            bm = st.text_input("月份", placeholder="609" if bc2=="CZCE" else "2609", key="bm")
            if st.button("批量添加", key="bb"):
                if not bm: st.warning("输月份")
                else:
                    a=0
                    for d in bs:
                        p2=d.split(" - ")[0]; i2=bp.get(p2,("自定义",10,1.0)); iid2=f"{p2}{bm}"
                        if iid2 in {i["instrument_id"] for i in st.session_state.instruments}: continue
                        st.session_state.instruments.append({"exchange":bc2,"instrument_id":iid2,"label":f"{i2[0]}{bm}","size":i2[1],"price_tick":i2[2],"group":i2[0]})
                        a+=1
                    if a: save_state(); st.success(f"添加{a}个"); st.rerun()
    with st.expander("品种速查"):
        for ex,pr in PRODUCT_REFERENCE.items():
            st.markdown(f"**{ex}**")
            st.markdown(" ".join([f"`{c}`{i[0]}" for c,i in pr.items()]))
    st.markdown("---")
    with st.expander("天勤量化账号", expanded=not st.session_state.get("tq_user")):
        st.caption("免费注册: https://www.shinnytech.com/register")
        _tq_u = st.text_input("天勤账号", value=st.session_state.get("tq_user", ""), key="tq_user_input")
        _tq_p = st.text_input("天勤密码", value=st.session_state.get("tq_pass", ""), type="password", key="tq_pass_input")
        if _tq_u != st.session_state.get("tq_user", "") or _tq_p != st.session_state.get("tq_pass", ""):
            st.session_state.tq_user = _tq_u
            st.session_state.tq_pass = _tq_p
            save_state()
        if st.session_state.get("tq_user"):
            if "_tq_api" in st.session_state and st.session_state._tq_api is not None:
                st.success("天勤已连接")
            else:
                st.caption("下次刷新时自动连接")
    st.markdown("---")
    st.markdown("## 自动交易")
    st.session_state.auto_trading = st.toggle("启用自动交易", value=False, key="at_toggle")
    if st.session_state.auto_trading:
        st.session_state.auto_trade_timeframe = st.selectbox(
            "策略周期",
            options=["1m", "5m", "15m", "1d"],
            format_func=lambda x: {"1m": "1分钟", "5m": "5分钟", "15m": "15分钟", "1d": "日线"}[x],
            index=["1m", "5m", "15m", "1d"].index(st.session_state.get("auto_trade_timeframe", "1m")),
            key="at_tf",
            help="自动交易使用的K线周期，信号基于该周期的技术指标计算"
        )
        st.session_state.signal_threshold = st.slider("信号阈值", 10, 100, 50, key="at_threshold")
        st.session_state.stop_loss_pct = st.slider("止损 %", 0.5, 10.0, 2.0, step=0.5, key="at_sl")
        st.session_state.take_profit_pct = st.slider("止盈 %", 0.5, 20.0, 4.0, step=0.5, key="at_tp")
        st.session_state.trade_volume = st.number_input("每次手数", 1, 100, 1, key="at_vol")
        st.session_state.reversal_threshold = st.slider("信号反转平仓阈值", 10, 80, 30, key="at_rv",
            help="信号反向超过此值时主动平仓 (如做多时信号变<-30则平仓)")
        st.caption("综合信号超过阈值时自动开仓；信号反转或触及止盈止损时平仓")
        save_state()
    else:
        save_state()  # 关闭自动交易时也保存（记住关闭状态）
    st.markdown("---")
    with st.expander(f"交易日志 ({len(st.session_state.trade_log)})"):
        if st.session_state.trade_log:
            for t in reversed(st.session_state.trade_log[-30:]):
                icon = "🟢" if t["direction"] == "buy" else "🔴"
                st.markdown(f"{icon} **{t['time']}** {t['instrument_id']}")
                st.caption(f"{t['action']} {t['volume']}手 @ {t['price']} | SL:{t['stop_loss']} TP:{t['take_profit']}")
                if t.get("message"):
                    st.caption(f"  {t['message']}")
        else:
            st.caption("暂无交易记录")
    st.markdown("---")
    st.caption("💾 品种/持仓/参数已自动保存，重启后自动恢复")
    st.markdown("---")
    with st.expander("版本与更新"):
        st.caption(f"当前版本: {DASHBOARD_VERSION}")
        update_url_input = st.text_input(
            "更新地址",
            value=st.session_state.get("update_url", UPDATE_URL),
            key="update_url_input",
            help="HTTP地址或本地/网络路径，指向 update.json",
            placeholder="https://example.com/update.json 或 \\\\server\\share\\update.json"
        )
        if update_url_input != st.session_state.get("update_url", ""):
            st.session_state.update_url = update_url_input
            save_state()
        if st.button("检查更新", key="btn_check_update"):
            with st.spinner("检查中..."):
                has_update, remote_ver, download_url = check_for_update(st.session_state.get("update_url", UPDATE_URL))
                if has_update:
                    st.success(f"发现新版本: {remote_ver}")
                    if st.button("立即更新", key="btn_do_update", type="primary"):
                        success, msg = download_update(download_url)
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                elif remote_ver and not has_update:
                    st.info(f"已是最新版本 ({remote_ver})")
                elif remote_ver and "http" not in str(remote_ver) and "://" not in str(remote_ver):
                    st.error(f"检查失败: {remote_ver}")
                else:
                    st.warning("未配置更新地址或无法连接")
    st.markdown("---")
    if st.button("清除保存数据", key="btn_reset"):
        import os
        try: os.remove(STATE_FILE)
        except: pass
        st.session_state.clear()
        st.rerun()


# ============================================================================
# 顶部
# ============================================================================

# 交易时间状态
_in_trading = is_trading_hours()
_next_open = get_next_open_time()

c1,c2,c3,c4,c5 = st.columns([3,2,2,2,1])
with c1: st.markdown("## 订单流量化分析面板")
with c2:
    if _in_trading:
        st.success("交易中", icon="🟢")
    else:
        st.warning(f"休市中 (下次: {_next_open})", icon="🟡")
with c3:
    # MCP 连接状态
    if st.session_state.connected:
        st.success("MCP 已连接", icon="🔗")
    else:
        if st.button("连接 MCP", key="btn_mcp_connect"):
            data = one_shot_fetch(st.session_state.instruments[:1])
            if data:
                st.session_state.connected = True
                st.session_state.start_time = datetime.now()
                st.rerun()
            else:
                st.error("连接失败, 确认无限易MCPStrategy已启动")
with c4:
    st.caption(f"更新: {datetime.now().strftime('%H:%M:%S')}")
    if st.session_state.start_time:
        u=datetime.now()-st.session_state.start_time
        h,r2=divmod(int(u.total_seconds()),3600); m,s=divmod(r2,60)
        st.caption(f"运行: {h:02d}:{m:02d}:{s:02d}")
with c5: auto_refresh = st.toggle("自动刷新", value=True, key="ar")


# ============================================================================
# 自动交易启用时自动连接 MCP
# ============================================================================

if st.session_state.get("auto_trading") and not st.session_state.connected:
    # 尝试自动连接 MCP (仅交易时间内)
    if _in_trading:
        with st.spinner("自动交易已启用，正在连接 MCP..."):
            test_data = one_shot_fetch(st.session_state.instruments[:1])
            if test_data:
                st.session_state.connected = True
                st.session_state.start_time = datetime.now()
                st.rerun()
            else:
                st.warning("自动交易已启用，但 MCP 连接失败。行情数据使用天勤量化。")
    else:
        st.info(f"自动交易已启用，当前休市中。行情数据使用天勤量化。下段交易时间: {_next_open}")


# ============================================================================
# 数据获取 - 天勤为主, MCP 为补充
# ============================================================================

_data_source = "tq"  # 默认使用天勤

# 检查天勤是否已配置
_tq_ready = bool(st.session_state.get("tq_user") and st.session_state.get("tq_pass"))
if not _tq_ready:
    st.warning("请先在侧边栏配置天勤量化账号 (免费注册: https://www.shinnytech.com/register)")

if st.session_state.connected and st.session_state.instruments:
    # MCP 已连接: 尝试用 MCP 获取数据 (更精确的合约数据)
    with st.spinner("获取数据中 (MCP)..."):
        mcp_data = one_shot_fetch(st.session_state.instruments)
        if mcp_data:
            st.session_state.data = mcp_data
            _data_source = "mcp"
        elif _tq_ready:
            st.warning("MCP 数据获取失败，切换到天勤量化")
            _tq_data = fetch_data_from_tq(st.session_state.instruments)
            if _tq_data:
                st.session_state.data = _tq_data
                _data_source = "tq"
            else:
                st.error("数据获取全部失败")
        else:
            st.error("MCP 数据获取失败，请先配置天勤量化账号或使用MCP")
elif st.session_state.instruments:
    # MCP 未连接: 使用天勤数据
    if _tq_ready:
        with st.spinner("获取数据中 (天勤)..."):
            _tq_data = fetch_data_from_tq(st.session_state.instruments)
            if _tq_data:
                st.session_state.data = _tq_data
            else:
                # 显示具体错误原因
                _api, _err = _get_tq_api()
                if _err == "NEED_AUTH":
                    st.warning("请先在侧边栏配置天勤量化账号")
                elif _err == "AUTH_FAILED":
                    st.error("天勤账号或密码错误，请在侧边栏重新输入")
                elif _err:
                    st.warning(f"天勤: {_err}")
                else:
                    st.warning("天勤数据获取失败，请检查网络")
    else:
        st.session_state.data = {}

# 显示数据来源
if st.session_state.data:
    if _data_source == "mcp":
        st.caption("📡 数据来源: MCP (无限易)")
    else:
        st.caption("📡 数据来源: 天勤量化")

# 获取资金和持仓 (仅 MCP 连接时)
if st.session_state.connected:
    st.session_state.capital_info = None
    st.session_state.holdings_info = {}
    try:
        time.sleep(0.5)
        cap_result = send_trade("fetch_capital", {})
        if cap_result.get("ok") and isinstance(cap_result.get("data"), dict):
            st.session_state.capital_info = cap_result["data"]
        time.sleep(0.5)
        hold_result = send_trade("fetch_all_holdings", {"simple": False})
        if hold_result.get("ok") and isinstance(hold_result.get("data"), dict):
            holdings = {}
            for investor, contracts in hold_result["data"].items():
                if not isinstance(contracts, dict):
                    continue
                for inst_id, hedgeflags in contracts.items():
                    if not isinstance(hedgeflags, dict):
                        continue
                    for hf, pos_data in hedgeflags.items():
                        if isinstance(pos_data, str):
                            try:
                                pos_data = ast.literal_eval(pos_data)
                            except Exception:
                                continue
                        if isinstance(pos_data, dict):
                            long_pos = pos_data.get("long", {})
                            short_pos = pos_data.get("short", {})
                            net = pos_data.get("net_position", 0)
                            if net != 0:
                                direction = "buy" if net > 0 else "sell"
                                side = long_pos if net > 0 else short_pos
                                if isinstance(side, dict) and side.get("position", 0) > 0:
                                    holdings[inst_id] = {
                                        "direction": direction,
                                        "volume": abs(net),
                                        "open_avg_price": side.get("open_avg_price", 0),
                                        "position_avg_price": side.get("position_avg_price", 0),
                                        "position_profit": side.get("position_profit", 0),
                                        "used_margin": side.get("used_margin", 0),
                                        "close_available": side.get("close_available", 0),
                                    }
            st.session_state.holdings_info = holdings
    except Exception:
        pass


# ============================================================================
# 账户资金
# ============================================================================

if st.session_state.capital_info:
    cap = st.session_state.capital_info
    st.markdown("---")
    cc1, cc2, cc3, cc4, cc5 = st.columns(5)
    with cc1:
        st.metric("账户权益", f"¥{cap.get('balance', 0):,.2f}")
    with cc2:
        st.metric("可用资金", f"¥{cap.get('available', 0):,.2f}")
    with cc3:
        st.metric("保证金", f"¥{cap.get('margin', 0):,.2f}")
    with cc4:
        pnl = cap.get("position_profit", 0)
        st.metric("持仓盈亏", f"¥{pnl:+,.2f}", delta=f"{pnl:+,.2f}")
    with cc5:
        risk = cap.get("risk", 0) * 100
        st.metric("风险度", f"{risk:.2f}%")


# ============================================================================
# 实际持仓
# ============================================================================

if st.session_state.holdings_info:
    st.markdown("---")
    st.markdown("#### 实际持仓")
    holdings = st.session_state.holdings_info
    hcols = st.columns(min(len(holdings), 4))
    for hi, (inst_id, hinfo) in enumerate(holdings.items()):
        with hcols[hi % len(hcols)]:
            d = hinfo["direction"]
            dir_label = "🟢多" if d == "buy" else "🔴空"
            vol = hinfo["volume"]
            avg_price = hinfo["open_avg_price"]
            pnl = hinfo["position_profit"]
            margin = hinfo["used_margin"]
            pnl_color = "green" if pnl >= 0 else "red"

            # 获取当前价格
            _sd = st.session_state.data or {}
            tick_data = _sd.get(inst_id, {}).get("tick", {})
            cp = tick_data.get("last_price", 0) if isinstance(tick_data, dict) else 0

            st.markdown(f"**{inst_id}** &nbsp; {dir_label} {vol}手")
            if cp > 0:
                st.markdown(f"现价: **{cp:.0f}** | 开仓均价: {avg_price:.1f}")
            else:
                st.markdown(f"开仓均价: **{avg_price:.1f}**")
            st.markdown(f"浮动盈亏: <span style='color:{pnl_color};font-weight:bold;font-size:1.2em'>{pnl:+,.0f}</span>", unsafe_allow_html=True)
            st.caption(f"保证金: ¥{margin:,.0f}")

            # 检查是否有我们记录的SL/TP
            tracked = st.session_state.positions.get(inst_id, {})
            if tracked:
                sl = tracked.get("sl", 0)
                tp = tracked.get("tp", 0)
                st.caption(f"止损: {sl:.0f} | 止盈: {tp:.0f}")


# ============================================================================
# 价格卡片 (MCP + 天勤实时补充)
# ============================================================================

st.markdown("---")

# 获取天勤实时行情作为补充 (仅在MCP数据源时, 避免重复wait_update)
tq_quotes = {}
if st.session_state.instruments and _data_source == "mcp":
    try:
        tq_quotes = tq_realtime(st.session_state.instruments, cache_sec=1)
    except Exception:
        pass

groups = {}
for inst in st.session_state.instruments:
    g = inst["group"]
    if g not in groups: groups[g] = []
    groups[g].append(inst)

# 确保 data 不为 None
if st.session_state.data is None:
    st.session_state.data = {}

for gn, gi in groups.items():
    cols = st.columns(len(gi))
    for i, inst in enumerate(gi):
        iid = inst["instrument_id"]
        d = st.session_state.data.get(iid, {})
        tick = d.get("tick")
        tq = tq_quotes.get(iid, {})
        with cols[i]:
            # 优先MCP tick，无数据时用天勤补充
            if tick and "error" not in tick and tick.get("last_price", 0) > 0:
                p = tick.get("last_price",0); ps = tick.get("pre_settlement_price",0)
                ch = p-ps if ps else 0; cp = (ch/ps*100) if ps else 0
                prev = st.session_state.prev_ticks.get(iid, p)
                st.session_state.prev_ticks[iid] = p
                fl = "🔺" if p>prev else ("🔻" if p<prev else "")
                st.metric(label=f"{inst['label']} {fl}", value=f"{p:.0f}", delta=f"{ch:+.0f} ({cp:+.2f}%)")
                v=tick.get("volume",0); oi=tick.get("open_interest",0)
                a,b=st.columns(2)
                with a: st.caption(f"高{tick.get('high_price',0):.0f}/低{tick.get('low_price',0):.0f}")
                with b: st.caption(f"量{v:,}/仓{oi:,.0f}")
            elif tq and tq.get("last_price", 0) > 0:
                p = tq["last_price"]
                prev = st.session_state.prev_ticks.get(iid, p)
                st.session_state.prev_ticks[iid] = p
                fl = "🔺" if p>prev else ("🔻" if p<prev else "")
                st.metric(label=f"{inst['label']} {fl}📡", value=f"{p:.0f}", delta="天勤")
                a,b=st.columns(2)
                with a: st.caption(f"高{tq.get('high',0):.0f}/低{tq.get('low',0):.0f}")
                with b: st.caption(f"量{tq.get('volume',0):,}/仓{tq.get('hold',0):,}")
            else:
                st.metric(label=inst["label"], value="--", delta="无数据")


# ============================================================================
# K线走势图 (每个品种)
# ============================================================================

st.markdown("---")
st.markdown("#### K线走势")
_kl_tf = st.segmented_control("K线周期", ["1m", "5m", "1d"], default="1m", key="kl_tf",
    format_func=lambda x: {"1m": "1分钟", "5m": "5分钟", "1d": "日线"}[x])
_kl_map = {"1m": "candles_1m", "5m": "candles_5m", "1d": "candles_1d"}
_kl_key = _kl_map.get(_kl_tf, "candles_1m")
_kl_count = {"1m": 60, "5m": 40, "1d": 30}.get(_kl_tf, 60)

for gn, gi in groups.items():
    st.markdown(f"**{gn}**")
    kl_cols = st.columns(min(len(gi), 3))
    for ki, inst in enumerate(gi):
        iid = inst["instrument_id"]
        candles = st.session_state.data.get(iid, {}).get(_kl_key, [])
        with kl_cols[ki % len(kl_cols)]:
            if not candles:
                st.info(f"{inst['label']} 无K线数据")
                continue
            rc = candles[-_kl_count:]
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                row_heights=[0.72, 0.28], vertical_spacing=0.04)
            fig.add_trace(go.Candlestick(
                x=[c.get("time", "") for c in rc],
                open=[c.get("open", 0) for c in rc],
                high=[c.get("high", 0) for c in rc],
                low=[c.get("low", 0) for c in rc],
                close=[c.get("close", 0) for c in rc],
                increasing_line_color="#ef4444", decreasing_line_color="#22c55e",
                name="K线",
            ), row=1, col=1)
            vc = ["#ef4444" if c.get("close", 0) >= c.get("open", 0) else "#22c55e" for c in rc]
            fig.add_trace(go.Bar(
                x=[c.get("time", "") for c in rc],
                y=[c.get("volume", 0) for c in rc],
                marker_color=vc, name="成交量", showlegend=False,
            ), row=2, col=1)
            fig.update_layout(
                height=280, margin=dict(l=8, r=8, t=8, b=8),
                template="plotly_dark", showlegend=False,
                xaxis_rangeslider_visible=False,
                xaxis2_rangeslider_visible=False,
            )
            fig.update_xaxes(type="category", nticks=6, row=1, col=1)
            fig.update_xaxes(type="category", nticks=6, row=2, col=1)
            st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# 持仓监控
# ============================================================================

if st.session_state.connected and st.session_state.positions:
    st.markdown("---")
    st.markdown("#### 当前持仓")
    pos_cols = st.columns(min(len(st.session_state.positions), 4))
    for pi, (iid, pos) in enumerate(st.session_state.positions.items()):
        with pos_cols[pi % len(pos_cols)]:
            d = st.session_state.data.get(iid, {})
            tick = d.get("tick") if isinstance(d, dict) else None
            cp = tick.get("last_price", 0) if isinstance(tick, dict) else 0
            entry = pos["entry_price"]
            direction = pos["direction"]
            vol = pos["volume"]
            sl = pos["sl"]
            tp = pos["tp"]
            dir_label = "🟢多" if direction == "buy" else "🔴空"
            if cp > 0:
                pnl = (cp - entry) * vol if direction == "buy" else (entry - cp) * vol
                pnl_color = "green" if pnl > 0 else "red"
                st.markdown(f"**{iid}** {dir_label} {vol}手")
                st.markdown(f"入场: {entry:.0f} | 现价: {cp:.0f}")
                st.markdown(f"盈亏: <span style='color:{pnl_color}'>{pnl:+.0f}</span>", unsafe_allow_html=True)
                st.caption(f"止损: {sl:.0f} | 止盈: {tp:.0f}")
                # 进度条显示距离止盈止损
                if direction == "buy":
                    sl_dist = (entry - cp) / (entry - sl) * 100 if entry > sl else 0
                    tp_dist = (cp - entry) / (tp - entry) * 100 if tp > entry else 0
                else:
                    sl_dist = (cp - entry) / (sl - entry) * 100 if sl > entry else 0
                    tp_dist = (entry - cp) / (entry - tp) * 100 if entry > tp else 0
                st.progress(min(max(sl_dist / 100, 0), 1), text=f"止损距离 {sl_dist:.0f}%")
            else:
                st.markdown(f"**{iid}** {dir_label} {vol}手")
                st.caption(f"入场: {entry:.0f} | 等待行情...")


# ============================================================================
# Tabs
# ============================================================================

st.markdown("---")

# 预取机构持仓数据供信号计算使用 (不依赖MCP连接, 使用akshare从交易所网站获取)
_institutional_rank = {}
_institutional_rank_error = None
if st.session_state.instruments:
    try:
        _institutional_rank = fetch_position_rank(st.session_state.instruments, top_n=5)
    except Exception as e:
        _institutional_rank_error = str(e)

tab1,tab2,tab3,tab4,tab5,tab7,tab6 = st.tabs(["订单流Delta","量价仓","主力价位","能量","信号","机构持仓","期权交易"])

with tab1:
    st.markdown("#### Delta分析")
    for gn,gi in groups.items():
        st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
        for ci,inst in enumerate(gi):
            iid=inst["instrument_id"]; candles=st.session_state.data.get(iid,{}).get("candles_1m",[])
            with cols[ci]:
                if not candles: st.info(f"{inst['label']}: 无数据"); continue
                ds=calc_delta(candles)
                if not ds: st.info("无Delta"); continue
                rc=ds[-10:]; dv=[d["delta"] for d in rc]; cd=sum(dv)
                cl2=["#ef4444" if d>=0 else "#22c55e" for d in dv]
                fig=go.Figure(go.Bar(x=[d["time"][-8:] for d in rc],y=dv,marker_color=cl2))
                fig.update_layout(title=f"{inst['label']} Delta",height=220,margin=dict(l=10,r=10,t=35,b=10),xaxis_tickangle=-45,template="plotly_dark",showlegend=False)
                st.plotly_chart(fig,use_container_width=True)
                m1,m2,m3=st.columns(3)
                with m1: st.metric("最新",f"{rc[-1]['delta']:+,}")
                with m2: st.metric("累计",f"{cd:+,}")
                with m3: st.metric("多空比",f"{sum(max(0,d) for d in dv)/max(sum(abs(min(0,d)) for d in dv),1):.2f}")

with tab2:
    st.markdown("#### 量价仓分析")
    for gn,gi in groups.items():
        st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
        for ci,inst in enumerate(gi):
            iid=inst["instrument_id"]; candles=st.session_state.data.get(iid,{}).get("candles_1m",[])
            with cols[ci]:
                if not candles: st.info("无数据"); continue
                vpo=analyze_vpo(candles)
                ic="🟢" if vpo["score"]>30 else ("🔴" if vpo["score"]<-30 else "🟡")
                st.markdown(f"{ic} **{vpo['state']}**"); st.metric("评分",f"{vpo['score']:+d}")
                a,b=st.columns(2)
                with a: st.caption(f"放量:{vpo['vol_ratio']:.2f}x")
                with b: st.caption(f"价格:{vpo['price_change']:+.1f}")
                rc=candles[-20:]
                fig=make_subplots(rows=2,cols=1,shared_xaxes=True,row_heights=[0.7,0.3],vertical_spacing=0.05)
                fig.add_trace(go.Candlestick(x=[c.get("time","") for c in rc],open=[c.get("open",0) for c in rc],high=[c.get("high",0) for c in rc],low=[c.get("low",0) for c in rc],close=[c.get("close",0) for c in rc],increasing_line_color="#ef4444",decreasing_line_color="#22c55e",name="K"),row=1,col=1)
                vc=["#ef4444" if c.get("close",0)>=c.get("open",0) else "#22c55e" for c in rc]
                fig.add_trace(go.Bar(x=[c.get("time","") for c in rc],y=[c.get("volume",0) for c in rc],marker_color=vc,name="量"),row=2,col=1)
                fig.update_layout(height=300,margin=dict(l=10,r=10,t=10,b=10),template="plotly_dark",showlegend=False)
                st.plotly_chart(fig,use_container_width=True)

with tab3:
    st.markdown("#### 主力价位 (增强版)")

    # 子Tab: 成交量分布 | POC迁移 | 多周期POC | 历史POC
    poc_sub1, poc_sub2, poc_sub3, poc_sub4 = st.tabs(["成交量分布", "POC迁移追踪", "多周期POC", "历史POC"])

    with poc_sub1:
        st.markdown("#### 成交量分布 + 关键价位")
        for gn,gi in groups.items():
            st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
            for ci,inst in enumerate(gi):
                iid=inst["instrument_id"]; d=st.session_state.data.get(iid,{}); candles=d.get("candles_1m",[]); tick=d.get("tick",{})
                with cols[ci]:
                    if not candles: st.info("无数据"); continue
                    prof=calc_volume_profile(candles); cp=tick.get("last_price",0) if isinstance(tick,dict) else 0
                    lv=find_key_levels(prof,cp)
                    if prof:
                        sp=sorted(prof.items(),key=lambda x:x[0]); ps=[p for p,v in sp]; vs=[v for p,v in sp]
                        bc=["#6b7280"]*len(ps); pp=lv["poc"]["price"] if lv["poc"] else None
                        for idx,p in enumerate(ps):
                            if pp and abs(p-pp)<2: bc[idx]="#f59e0b"
                            elif lv["support"] and abs(p-lv["support"]["price"])<2: bc[idx]="#22c55e"
                            elif lv["resistance"] and abs(p-lv["resistance"]["price"])<2: bc[idx]="#ef4444"
                        fig=go.Figure(go.Bar(x=vs,y=ps,orientation="h",marker_color=bc))
                        if cp>0: fig.add_hline(y=cp,line_dash="dash",line_color="white",line_width=2,annotation_text=f"现价{cp:.0f}")
                        fig.update_layout(title=f"{inst['label']}量分布",height=300,margin=dict(l=10,r=10,t=35,b=10),template="plotly_dark",showlegend=False)
                        st.plotly_chart(fig,use_container_width=True)
                    m1,m2,m3=st.columns(3)
                    with m1: st.metric("压力",f"{lv['resistance']['price']:.0f}" if lv["resistance"] else "--")
                    with m2: st.metric("POC",f"{lv['poc']['price']:.0f}" if lv["poc"] else "--")
                    with m3: st.metric("支撑",f"{lv['support']['price']:.0f}" if lv["support"] else "--")

    with poc_sub2:
        st.markdown("#### POC 迁移追踪 (主力成本移动)")
        for gn,gi in groups.items():
            st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
            for ci,inst in enumerate(gi):
                iid=inst["instrument_id"]; d=st.session_state.data.get(iid,{}); candles=d.get("candles_1m",[]); tick=d.get("tick",{})
                with cols[ci]:
                    if not candles or len(candles) < 15: st.info("数据不足"); continue
                    migration = calc_poc_migration(candles, window=10)
                    cp=tick.get("last_price",0) if isinstance(tick,dict) else 0

                    # 显示迁移方向
                    dir_color = "#ef4444" if "上移" in migration["direction"] else ("#22c55e" if "下移" in migration["direction"] else "#f59e0b")
                    st.markdown(f"**{inst['label']}** POC: {migration['direction']} ({migration['trend']:+.2f}%)")

                    if migration["migration"]:
                        # 绘制 POC 迁移图
                        mig_data = migration["migration"][-30:]  # 最近30个点
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=list(range(len(mig_data))),
                            y=[m["price"] for m in mig_data],
                            mode="lines+markers",
                            line=dict(color="#f59e0b", width=2),
                            marker=dict(size=4),
                            name="POC"
                        ))
                        if cp > 0:
                            fig.add_hline(y=cp, line_dash="dash", line_color="white", annotation_text=f"现价{cp:.0f}")
                        fig.update_layout(
                            height=200, margin=dict(l=10,r=10,t=10,b=10),
                            template="plotly_dark", showlegend=False,
                            xaxis=dict(showticklabels=False)
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    m1,m2,m3=st.columns(3)
                    with m1: st.metric("起始POC", f"{migration.get('first_poc', 0):.0f}" if migration.get('first_poc') else "--")
                    with m2: st.metric("当前POC", f"{migration.get('last_poc', 0):.0f}" if migration.get('last_poc') else "--")
                    with m3: st.metric("偏移", f"{migration['trend']:+.2f}%")

    with poc_sub3:
        st.markdown("#### 多周期 POC 对比")
        for gn,gi in groups.items():
            st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
            for ci,inst in enumerate(gi):
                iid=inst["instrument_id"]; d=st.session_state.data.get(iid,{}); tick=d.get("tick",{})
                with cols[ci]:
                    cp=tick.get("last_price",0) if isinstance(tick,dict) else 0
                    mt_poc = calc_multi_timeframe_poc(d)

                    st.markdown(f"**{inst['label']}**")

                    # 表格显示多周期POC
                    poc_rows = []
                    for tf_name, tf_key in [("1分钟", "1m"), ("5分钟", "5m"), ("日线", "1d")]:
                        poc_data = mt_poc.get(tf_key)
                        if poc_data:
                            diff_pct = (poc_data["price"] - cp) / cp * 100 if cp > 0 else 0
                            pos = "上方" if diff_pct > 0 else "下方"
                            poc_rows.append({
                                "周期": tf_name,
                                "POC": f"{poc_data['price']:.0f}",
                                "成交量": f"{poc_data['volume']:,}",
                                "与现价": f"{pos} {abs(diff_pct):.2f}%"
                            })
                        else:
                            poc_rows.append({"周期": tf_name, "POC": "--", "成交量": "--", "与现价": "--"})

                    st.dataframe(pd.DataFrame(poc_rows), hide_index=True, use_container_width=True)

                    # 判断多周期共振
                    valid_pocs = [mt_poc[k]["price"] for k in ["1m","5m","1d"] if mt_poc.get(k)]
                    if len(valid_pocs) >= 2 and cp > 0:
                        max_diff = max(abs(p - cp) for p in valid_pocs)
                        if max_diff / cp * 100 < 0.5:
                            st.success("多周期POC共振 - 关键价位!")
                        elif all(p > cp for p in valid_pocs):
                            st.info("现价在所有周期POC下方 - 潜在支撑")
                        elif all(p < cp for p in valid_pocs):
                            st.warning("现价在所有周期POC上方 - 潜在压力")

    with poc_sub4:
        st.markdown("#### 历史 POC 关键价位")
        for gn,gi in groups.items():
            st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
            for ci,inst in enumerate(gi):
                iid=inst["instrument_id"]; d=st.session_state.data.get(iid,{}); candles_1d=d.get("candles_1d",[]); tick=d.get("tick",{})
                with cols[ci]:
                    cp=tick.get("last_price",0) if isinstance(tick,dict) else 0
                    hist = calc_historical_poc(candles_1d, days=5)

                    st.markdown(f"**{inst['label']}**")

                    if hist["yesterday"]:
                        y = hist["yesterday"]
                        diff = (y["poc"] - cp) / cp * 100 if cp > 0 else 0
                        st.metric("昨日POC", f"{y['poc']:.0f}", f"{diff:+.2f}%")

                        # 显示历史POC列表
                        if hist["prev_days"]:
                            hist_rows = []
                            for pd_data in hist["prev_days"][-4:]:
                                d_pct = (pd_data["poc"] - cp) / cp * 100 if cp > 0 else 0
                                hist_rows.append({
                                    "日期": pd_data["date"],
                                    "POC": f"{pd_data['poc']:.0f}",
                                    "区间": f"{pd_data['low']:.0f}-{pd_data['high']:.0f}",
                                    "与现价": f"{d_pct:+.2f}%"
                                })
                            st.caption("近几日POC:")
                            st.dataframe(pd.DataFrame(hist_rows), hide_index=True, use_container_width=True)
                    else:
                        st.info("无历史数据")

with tab4:
    st.markdown("#### 能量解读")
    for gn,gi in groups.items():
        st.markdown(f"**{gn}**"); cols=st.columns(len(gi))
        for ci,inst in enumerate(gi):
            iid=inst["instrument_id"]; candles=st.session_state.data.get(iid,{}).get("candles_1m",[])
            with cols[ci]:
                if not candles: st.info("无数据"); continue
                ds=calc_delta(candles); mom=analyze_momentum(ds)
                ti="🟢" if "上涨" in mom["trend"] or "反弹" in mom["trend"] else ("🔴" if "下跌" in mom["trend"] or "回调" in mom["trend"] else "🟡")
                st.markdown(f"{ti} **{mom['trend']}**")
                m1,m2,m3=st.columns(3)
                with m1: st.metric("动量",f"{mom['score']:+.1f}")
                with m2: st.metric("多空比",f"{mom['ratio']:.2f}")
                with m3: st.metric("多/空",f"{mom['buy_energy']:,}/{mom['sell_energy']:,}")
                if len(ds)>=5:
                    rc=ds[-15:]
                    fig=go.Figure()
                    fig.add_trace(go.Bar(x=[d["time"][-8:] for d in rc],y=[d["buy_vol"] for d in rc],name="多",marker_color="#ef4444"))
                    fig.add_trace(go.Bar(x=[d["time"][-8:] for d in rc],y=[-d["sell_vol"] for d in rc],name="空",marker_color="#22c55e"))
                    fig.update_layout(barmode="relative",height=200,margin=dict(l=10,r=10,t=10,b=10),template="plotly_dark",showlegend=True,legend=dict(orientation="h",y=1.1))
                    st.plotly_chart(fig,use_container_width=True)

with tab5:
    st.markdown("#### 综合信号 (含机构持仓)")
    rows=[]
    for inst in st.session_state.instruments:
        iid=inst["instrument_id"]; d=st.session_state.data.get(iid,{}); candles=d.get("candles_1m",[]); tick=d.get("tick",{})
        cp=tick.get("last_price",0) if isinstance(tick,dict) else 0
        if not candles: rows.append({"合约":inst["label"],"现价":cp or "--","订单流":"--","量价仓":"--","主力":"--","能量":"--","机构":"--","总分":"--","方向":"--"}); continue
        ds=calc_delta(candles); prof=calc_volume_profile(candles); lv=find_key_levels(prof,cp)
        of=max(-100,min(100,sum(dd["delta"] for dd in ds[-5:])/max(len(ds[-5:]),1)*2))
        vpo=analyze_vpo(candles); vs=vpo["score"]; kl=0
        if lv["support"] and cp and abs(cp-lv["support"]["price"])/cp*100<0.5: kl=40
        if lv["resistance"] and cp and abs(cp-lv["resistance"]["price"])/cp*100<0.5: kl=-40
        mom=analyze_momentum(ds); ms=mom["score"]
        # 机构持仓信号
        inst_sig = 0
        rd = _institutional_rank.get(iid)
        if rd and rd.get("longs") and rd.get("shorts"):
            tl = sum(l["hold"] for l in rd["longs"])
            ts = sum(s["hold"] for s in rd["shorts"])
            tcl = sum(l["chg"] for l in rd["longs"])
            tcs = sum(s["chg"] for s in rd["shorts"])
            if tl + ts > 0:
                ratio = (tl - ts) / (tl + ts) * 100  # -100~+100
                chg_sig = max(-50, min(50, (tcl - tcs) / max(tl + ts, 1) * 500))
                inst_sig = max(-100, min(100, ratio * 0.6 + chg_sig * 0.4))
        # 综合: 订单流25% + 量价仓20% + 主力15% + 能量20% + 机构20%
        total=max(-100,min(100,of*0.25+vs*0.20+kl*0.15+ms*0.20+inst_sig*0.20))
        dr="🟢多" if total>30 else ("🔴空" if total<-30 else "🟡中性")
        inst_label = f"{inst_sig:+.0f}" if rd else "--"
        rows.append({"合约":inst["label"],"现价":f"{cp:.0f}" if cp else "--","订单流":f"{of:+.0f}","量价仓":f"{vs:+d}","主力":f"{kl:+.0f}","能量":f"{ms:+.1f}","机构":inst_label,"总分":f"{total:+.1f}","方向":dr})
    st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)
    st.caption("信号权重: 订单流25% | 量价仓20% | 能量20% | 机构20% | 主力15%")

with tab7:
    st.markdown("#### 机构多空持仓排名 (Top 5)")
    st.caption("数据来源: 交易所每日持仓排名 (T-1日数据, 通过akshare获取, 无需MCP连接)")
    if st.session_state.instruments:
        # 强制刷新按钮
        if st.button("🔄 强制刷新持仓排名", key="refresh_rank"):
            if "_rank_cache" in st.session_state:
                st.session_state._rank_cache = {"fetch_time": 0, "data": {}, "exchanges": set(), "failed": {}}
            _institutional_rank = fetch_position_rank(st.session_state.instruments, top_n=5)
            st.rerun()

        if _institutional_rank_error:
            st.error(f"获取数据出错: {_institutional_rank_error}")

        rank_data = _institutional_rank
        if rank_data:
            # 显示有数据的合约
            found_any = False
            for inst in st.session_state.instruments:
                iid = inst["instrument_id"]
                rd = rank_data.get(iid)
                if not rd:
                    continue
                found_any = True
                st.markdown(f"**{inst['label']}** ({iid}) — 数据日期: {rd.get('date', '--')}")
                c_l, c_s = st.columns(2)
                with c_l:
                    st.markdown("##### 🟢 多头前5")
                    if rd["longs"]:
                        long_rows = []
                        for i, l in enumerate(rd["longs"]):
                            chg_color = "🔺" if l["chg"] > 0 else ("🔻" if l["chg"] < 0 else "➖")
                            long_rows.append({"排名": i+1, "机构": l["name"], "持仓": f"{l['hold']:,}", "增减": f"{chg_color}{l['chg']:+,}"})
                        st.dataframe(pd.DataFrame(long_rows), use_container_width=True, hide_index=True)
                    else:
                        st.info("无数据")
                with c_s:
                    st.markdown("##### 🔴 空头前5")
                    if rd["shorts"]:
                        short_rows = []
                        for i, s in enumerate(rd["shorts"]):
                            chg_color = "🔺" if s["chg"] > 0 else ("🔻" if s["chg"] < 0 else "➖")
                            short_rows.append({"排名": i+1, "机构": s["name"], "持仓": f"{s['hold']:,}", "增减": f"{chg_color}{s['chg']:+,}"})
                        st.dataframe(pd.DataFrame(short_rows), use_container_width=True, hide_index=True)
                    else:
                        st.info("无数据")

                # 多空对比小结
                total_long = sum(l["hold"] for l in rd["longs"]) if rd["longs"] else 0
                total_short = sum(s["hold"] for s in rd["shorts"]) if rd["shorts"] else 0
                net = total_long - total_short
                if total_long + total_short > 0:
                    ratio = total_long / (total_long + total_short) * 100
                    bias = "偏多" if ratio > 55 else ("偏空" if ratio < 45 else "均衡")
                    st.caption(f"多空比: {ratio:.1f}% (多{total_long:,} / 空{total_short:,} / 净{net:+,}) → {bias}")
                st.markdown("---")
            # 显示没有数据的合约
            no_data = [inst["label"] for inst in st.session_state.instruments if inst["instrument_id"] not in rank_data]
            if no_data:
                st.warning(f"以下合约暂无排名数据: {', '.join(no_data)} (可能该合约无排名或DCE数据暂不可用)")
            if not found_any:
                st.info("所有合约均无排名数据。请确认: 1) 品种为SHFE/CZCE上市品种; 2) 当前日期有历史排名数据。")
        else:
            st.info("暂无持仓排名数据。可能原因: 1) 网络无法访问交易所网站; 2) 当前日期无历史排名数据。请点击上方刷新按钮重试。")
    else:
        st.info("请先添加合约")

with tab6:
    st.markdown("#### 期权交易")

    # 品种选择
    OPT_PRODUCTS = {
        "SHFE": {
            "ag": ("白银期权", 15), "rb": ("螺纹期权", 10), "cu": ("铜期权", 5),
            "al": ("铝期权", 5), "au": ("黄金期权", 1000), "zn": ("锌期权", 5),
            "ni": ("镍期权", 1), "ru": ("橡胶期权", 10), "bu": ("沥青期权", 10),
        },
        "DCE": {
            "m": ("豆粕期权", 10), "i": ("铁矿期权", 100), "c": ("玉米期权", 10),
            "y": ("豆油期权", 10), "p": ("棕榈期权", 10), "a": ("豆一期权", 10),
            "l": ("塑料期权", 5), "pp": ("聚丙烯期权", 5), "v": ("PVC期权", 5),
            "eg": ("乙二醇期权", 10), "jm": ("焦煤期权", 60), "eb": ("苯乙烯期权", 5),
            "pg": ("液化气期权", 20),
        },
        "CZCE": {
            "SR": ("白糖期权", 10), "CF": ("棉花期权", 5), "TA": ("PTA期权", 5),
            "MA": ("甲醇期权", 10), "RM": ("菜粕期权", 10), "SA": ("纯碱期权", 20),
            "FG": ("玻璃期权", 20), "OI": ("菜油期权", 10), "AP": ("苹果期权", 10),
            "UR": ("尿素期权", 20), "PF": ("短纤期权", 5), "PK": ("花生期权", 5),
        },
    }
    oc1, oc2, oc3 = st.columns([1, 1, 1])
    with oc1:
        opt_ex = st.selectbox("交易所", list(OPT_PRODUCTS.keys()), key="opt_ex")
    with oc2:
        opt_prods = OPT_PRODUCTS.get(opt_ex, {})
        opt_prod_opts = [f"{k} - {v[0]}" for k, v in opt_prods.items()]
        opt_prod_sel = st.selectbox("品种", opt_prod_opts, key="opt_prod") if opt_prod_opts else ""
        opt_prod_id = opt_prod_sel.split(" - ")[0] if opt_prod_sel else ""
        opt_multiplier = opt_prods.get(opt_prod_id, ("", 10))[1] if opt_prod_id else 10
    with oc3:
        opt_vol = st.number_input("下单手数", 1, 100, 1, key="opt_vol")

    if opt_prod_id and st.session_state.connected:
        # 获取期权月份
        months_result = send_trade("fetch_option_months", {"exchange": opt_ex, "product_id": opt_prod_id})
        months = []
        if months_result.get("ok"):
            d = months_result.get("data")
            if isinstance(d, list):
                months = [str(m) if isinstance(m, str) else m.get("month", str(m)) if isinstance(m, dict) else str(m) for m in d]
            elif isinstance(d, dict): months = list(d.keys()) if d else []

        if months:
            oc4, oc5 = st.columns([1, 1])
            with oc4:
                opt_month = st.selectbox("标的月份", months, key="opt_month")
            # 获取行权价和ATM
            strikes_result = send_trade("fetch_option_strikes", {"exchange": opt_ex, "product_id": opt_prod_id, "underlying_symbol": opt_month})
            strikes = []
            if strikes_result.get("ok") and isinstance(strikes_result.get("data"), list):
                strikes = strikes_result["data"]

            # 获取标的现价
            underlying_tick = send_trade("fetch_tick", {"exchange": opt_ex, "instrument_id": opt_month})
            underlying_price = 0
            if underlying_tick.get("ok") and isinstance(underlying_tick.get("data"), dict):
                underlying_price = underlying_tick["data"].get("last_price", 0)
            if underlying_price <= 0:
                # 尝试从one_shot数据获取
                om_key = str(opt_month) if not isinstance(opt_month, str) else opt_month
                ud = st.session_state.data.get(om_key, {})
                ut = ud.get("tick") if isinstance(ud, dict) else None
                underlying_price = ut.get("last_price", 0) if isinstance(ut, dict) else 0

            with oc5:
                st.metric("标的现价", f"{underlying_price:.0f}" if underlying_price > 0 else "需订阅标的行情")

            if strikes and underlying_price > 0:
                # 找ATM附近行权价
                atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - underlying_price))
                # 显示ATM附近10档
                start_idx = max(0, atm_idx - 5)
                end_idx = min(len(strikes), atm_idx + 6)
                display_strikes = strikes[start_idx:end_idx]

                # 获取call和put合约
                calls_result = send_trade("fetch_option_calls", {"exchange": opt_ex, "product_id": opt_prod_id, "underlying_symbol": opt_month})
                puts_result = send_trade("fetch_option_puts", {"exchange": opt_ex, "product_id": opt_prod_id, "underlying_symbol": opt_month})
                calls = calls_result.get("data", []) if calls_result.get("ok") else []
                puts = puts_result.get("data", []) if puts_result.get("ok") else []

                if calls and puts:
                    # 订阅并获取期权行情
                    opt_ticks = {}
                    # 订阅标的
                    send_trade("start_quote_feed", {"exchange": opt_ex, "instrument_id": opt_month})
                    for strike in display_strikes:
                        si = strikes.index(strike)
                        if si < len(calls):
                            call_id = calls[si]
                            send_trade("start_quote_feed", {"exchange": opt_ex, "instrument_id": call_id})
                        if si < len(puts):
                            put_id = puts[si]
                            send_trade("start_quote_feed", {"exchange": opt_ex, "instrument_id": put_id})

                    time.sleep(1)  # 等行情数据到达

                    for strike in display_strikes:
                        si = strikes.index(strike)
                        if si < len(calls):
                            ct = send_trade("fetch_tick", {"exchange": opt_ex, "instrument_id": calls[si]})
                            if ct.get("ok") and isinstance(ct.get("data"), dict):
                                opt_ticks[calls[si]] = ct["data"]
                        if si < len(puts):
                            pt = send_trade("fetch_tick", {"exchange": opt_ex, "instrument_id": puts[si]})
                            if pt.get("ok") and isinstance(pt.get("data"), dict):
                                opt_ticks[puts[si]] = pt["data"]

                    # T型报价显示
                    st.markdown("---")
                    st.markdown(f"**{opt_month} 期权链** (平值附近)")

                    # 表头
                    h1, h2, h3, h4, h5, h6, h7 = st.columns([2, 1.5, 1.5, 1.2, 1.5, 1.5, 2])
                    with h1: st.markdown("**买Call**")
                    with h2: st.markdown("**Call卖价**")
                    with h3: st.markdown("**Call买价**")
                    with h4: st.markdown("**行权价**")
                    with h5: st.markdown("**Put买价**")
                    with h6: st.markdown("**Put卖价**")
                    with h7: st.markdown("**买Put**")

                    for strike in display_strikes:
                        si = strikes.index(strike)
                        is_atm = si == atm_idx
                        row_bg = "background:rgba(255,215,0,0.15);" if is_atm else ""

                        call_id = calls[si] if si < len(calls) else ""
                        put_id = puts[si] if si < len(puts) else ""
                        ct = opt_ticks.get(call_id, {})
                        pt = opt_ticks.get(put_id, {})

                        c_last = ct.get("last_price", 0) if ct else 0
                        c_bid = ct.get("bid_price1", 0) if ct else 0
                        c_ask = ct.get("ask_price1", 0) if ct else 0
                        p_last = pt.get("last_price", 0) if pt else 0
                        p_bid = pt.get("bid_price1", 0) if pt else 0
                        p_ask = pt.get("ask_price1", 0) if pt else 0

                        r1, r2, r3, r4, r5, r6, r7 = st.columns([2, 1.5, 1.5, 1.2, 1.5, 1.5, 2])
                        with r1:
                            if call_id:
                                if st.button(f"买Call", key=f"bc_{call_id}", help=f"买入开仓 {call_id}"):
                                    res = send_trade("submit_open", {"exchange": opt_ex, "instrument_id": call_id, "volume": opt_vol, "price": 0, "direction": "buy", "market": True, "memo": "手动买Call"})
                                    if res.get("ok"):
                                        st.success(f"买入 {call_id} {opt_vol}手 成功")
                                    else:
                                        st.error(f"买入失败: {res.get('error','')}")
                        with r2: st.markdown(f"<div style='{row_bg}text-align:center'>{c_ask:.1f}</div>", unsafe_allow_html=True)
                        with r3: st.markdown(f"<div style='{row_bg}text-align:center'>{c_bid:.1f}</div>", unsafe_allow_html=True)
                        with r4:
                            atm_mark = " ◀" if is_atm else ""
                            st.markdown(f"<div style='{row_bg}text-align:center;font-weight:bold'>{strike:.0f}{atm_mark}</div>", unsafe_allow_html=True)
                        with r5: st.markdown(f"<div style='{row_bg}text-align:center'>{p_bid:.1f}</div>", unsafe_allow_html=True)
                        with r6: st.markdown(f"<div style='{row_bg}text-align:center'>{p_ask:.1f}</div>", unsafe_allow_html=True)
                        with r7:
                            if put_id:
                                if st.button(f"买Put", key=f"bp_{put_id}", help=f"买入开仓 {put_id}"):
                                    res = send_trade("submit_open", {"exchange": opt_ex, "instrument_id": put_id, "volume": opt_vol, "price": 0, "direction": "buy", "market": True, "memo": "手动买Put"})
                                    if res.get("ok"):
                                        st.success(f"买入 {put_id} {opt_vol}手 成功")
                                    else:
                                        st.error(f"买入失败: {res.get('error','')}")

                    # 卖方操作
                    st.markdown("---")
                    st.markdown("**卖方操作** (卖出开仓)")
                    s1, s2, s3 = st.columns([1, 1, 1])
                    with s1:
                        sell_strike = st.selectbox("选择行权价", display_strikes, key="sell_strike")
                    with s2:
                        sell_type = st.radio("类型", ["卖出Call", "卖出Put"], key="sell_type", horizontal=True)
                    with s3:
                        if st.button("执行卖出开仓", type="primary", key="sell_opt_btn"):
                            si2 = strikes.index(sell_strike)
                            if sell_type == "卖出Call" and si2 < len(calls):
                                sid = calls[si2]
                            elif sell_type == "卖出Put" and si2 < len(puts):
                                sid = puts[si2]
                            else:
                                sid = ""
                            if sid:
                                res = send_trade("submit_open", {"exchange": opt_ex, "instrument_id": sid, "volume": opt_vol, "price": 0, "direction": "sell", "market": True, "memo": f"手动{sell_type}"})
                                if res.get("ok"):
                                    st.success(f"{sell_type} {sid} {opt_vol}手 成功")
                                else:
                                    st.error(f"卖出失败: {res.get('error','')}")
                else:
                    st.info("获取期权合约列表失败")
            else:
                st.info("获取行权价失败或标的行情未订阅")
        else:
            st.info("获取期权月份失败，请确认品种代码正确")
    elif not st.session_state.connected:
        st.info("请先连接 MCP")


# ============================================================================
# 自动交易执行
# ============================================================================

if st.session_state.get("auto_trading") and st.session_state.connected and st.session_state.data:
    threshold = st.session_state.get("signal_threshold", 50)
    sl_pct = st.session_state.get("stop_loss_pct", 2.0)
    tp_pct = st.session_state.get("take_profit_pct", 4.0)
    tvol = st.session_state.get("trade_volume", 1)
    trade_results = execute_auto_trades(
        st.session_state.data, st.session_state.instruments,
        threshold, sl_pct, tp_pct, tvol,
        timeframe=st.session_state.get("auto_trade_timeframe", "1m")
    )
    if trade_results:
        save_state()
        st.markdown("---")
        st.markdown("#### 自动交易执行")
        for tr in trade_results:
            icon = "🟢" if tr["direction"] == "buy" else "🔴"
            if "失败" in tr["action"]:
                st.error(f"{icon} {tr['time']} {tr['instrument_id']} {tr['action']}: {tr['message']}")
            else:
                st.success(f"{icon} {tr['time']} {tr['action']} {tr['instrument_id']} {tr['volume']}手 @ {tr['price']} | SL:{tr['stop_loss']} TP:{tr['take_profit']} | {tr['message']}")


# ============================================================================
# 止盈止损检查
# ============================================================================

if st.session_state.connected and st.session_state.positions and st.session_state.data:
    rv_threshold = st.session_state.get("reversal_threshold", 30)
    sl_tp_results = check_positions_sl_tp(
        st.session_state.data, st.session_state.instruments, rv_threshold,
        timeframe=st.session_state.get("auto_trade_timeframe", "1m")
    )
    if sl_tp_results:
        save_state()
        st.markdown("---")
        st.markdown("#### 止盈止损触发")
        for tr in sl_tp_results:
            icon = "🟢" if tr["direction"] == "buy" else "🔴"
            st.warning(f"{icon} {tr['time']} {tr['action']} {tr['instrument_id']} {tr['volume']}手 @ {tr['price']} | {tr['message']}")


# ============================================================================
# 自动刷新 (交易时间感知)
# ============================================================================

if auto_refresh:
    if _in_trading:
        # 交易时间内: 正常刷新 (2秒)
        time.sleep(2)
        st.rerun()
    else:
        # 休市: 降低刷新频率 (60秒), 仅刷新时间显示
        # 如果完全不需要刷新, 可以注释掉下面两行
        time.sleep(60)
        st.rerun()
