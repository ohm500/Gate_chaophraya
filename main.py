import sys
import os
import asyncio
import json
import math
from datetime import datetime
from typing import Optional

import socketio
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from gate_calculator import (
    calculate_gate_openings,
    SILL_ELEV,
    GATE_WIDTH,
    MAX_OPENING,
    G,
    DEEP_GATES,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI()

# 🔧 กำหนด origin ที่อนุญาตผ่าน environment variable
_allowed_origins_env = os.environ.get("ALLOWED_ORIGINS", "http://localhost:3000")
ALLOWED_ORIGINS = [origin.strip() for origin in _allowed_origins_env.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncClient()

latest_dam_data = {
    "status": "waiting",
    "updated_at": None,
    "wl_up": 15.86,
    "wl_down": 0,
    "flow": 0,
    "gates": {}
}

SOCKETIO_URL = "http://110.49.150.236"
SOCKETIO_RETRY_DELAY_SEC = 5  


@sio.event
async def connect():
    print("✅ [Socket.IO] เชื่อมต่อสำเร็จ! เริ่มดึงข้อมูล...")
    asyncio.create_task(request_report_loop())

@sio.event
async def disconnect():
    print("❌ [Socket.IO] หลุดการเชื่อมต่อ")

async def request_report_loop():
    await asyncio.sleep(1)
    while True:
        if sio.connected:
            try:
                today_str = datetime.now().strftime("%Y-%m-%d")
                await sio.emit('client-report', (today_str, today_str, "0", None, "0"))
            except Exception as e:
                print(f"เกิดข้อผิดพลาดตอนขอ Report: {e}")
        else:
            return
        await asyncio.sleep(15)

@sio.on('server-report')
async def on_server_report(data):
    global latest_dam_data
    try:
        if 'DATA' in data:
            raw_data = data['DATA']
            parsed_data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

            if isinstance(parsed_data, list) and len(parsed_data) > 0:
                latest_record = parsed_data[-1]
                latest_dam_data["status"] = "connected"

                if "LASTUPDATE" in latest_record:
                    latest_dam_data["updated_at"] = latest_record["LASTUPDATE"]

                if latest_record.get("WL_UP") is not None:
                    latest_dam_data["wl_up"] = float(latest_record.get("WL_UP"))
                if latest_record.get("WL_DOWN") is not None:
                    latest_dam_data["wl_down"] = float(latest_record.get("WL_DOWN"))
                if latest_record.get("FLOW") is not None:
                    latest_dam_data["flow"] = float(latest_record.get("FLOW"))

                for i in range(1, 17):
                    gate_key = f"G{i}"
                    if gate_key in latest_record:
                        latest_dam_data["gates"][f"GATE{i:02d}"] = float(latest_record.get(gate_key) or 0)

                print(f"📥 อัปเดตข้อมูลสำเร็จ! ระดับน้ำเหนือ: {latest_dam_data['wl_up']}")
    except Exception as e:
        print(f"Error parsing data: {e}")

async def start_socketio():
    while True:
        try:
            latest_dam_data["status"] = "waiting"
            print(f"กำลังเชื่อมต่อ Socket.IO ไปที่ {SOCKETIO_URL} ...")
            await sio.connect(SOCKETIO_URL, transports=['websocket'])
            await sio.wait()  
        except Exception as e:
            print(f"เชื่อมต่อล้มเหลว: {e}")

        latest_dam_data["status"] = "waiting"
        print(f"⏳ จะลองเชื่อมต่อ Socket.IO ใหม่ในอีก {SOCKETIO_RETRY_DELAY_SEC} วินาที...")
        await asyncio.sleep(SOCKETIO_RETRY_DELAY_SEC)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_socketio())

# ==============================================================
# Endpoint สำหรับหน้า Landing Page (ดึงข้อมูลน้ำ)
# ==============================================================
@app.get("/api/v1/realtime")
def get_realtime():
    return latest_dam_data

# ==============================================================
# Endpoints สำหรับหน้า โปรแกรมคำนวณ (ใช้คำนวณค่าต่างๆ)
# ==============================================================
@app.get("/api/v1/constants")
def get_constants():
    return {
        "sill_elev": SILL_ELEV,
        "gate_width": GATE_WIDTH,
        "max_opening": MAX_OPENING,
        "g": G,
        "cs_free": 0.65,       # ล็อกค่าให้ส่งกลับเป็น 0.65
        "cs_submerged": 0.80,  # ล็อกค่าให้ส่งกลับเป็น 0.80
        "deep_gates": DEEP_GATES,
    }

@app.get("/api/v1/calculate-flow")
def calculate_flow(
    q_target: float,
    delta_h: float,
    gate_status: str,
    wl_up: float = 15.86,
    wl_down: Optional[float] = None,
):
    if wl_down is None:
        wl_down = wl_up - delta_h

    status_list = [s == '1' for s in gate_status.split(',')]
    active_count = sum(status_list)

    if active_count == 0 or q_target <= 0 or delta_h <= 0:
        return {
            "status": "success",
            "results": {"target_opening_m": 0, "target_opening_cm": 0},
            "chart_data": [[i / 100.0, 0] for i in range(11)],
            "gate_openings": [{"หมายเลขบานระบาย": f"บานที่ {i+1}", "ระยะเปิด (เมตร)": 0.0, "ปริมาณน้ำ (ลบ.ม./วิ)": 0.0} for i in range(16)],
            "capacity_check": {
                "requested_q": q_target,
                "achieved_q": 0,
                "shortfall_q": max(0, q_target),
                "is_sufficient": q_target <= 0,
            }
        }

    total_active_width = active_count * GATE_WIDTH
    
    # 🚨 แก้บั๊กเปลี่ยนจาก CS_SUBMERGED เป็นเลข 0.80 โดยตรง
    a_target = q_target / (0.80 * math.sqrt(2 * G * delta_h)) if delta_h > 0 else 0
    target_opening_m = a_target / total_active_width if total_active_width > 0 else 0

    curve_points = []
    for i in range(0, 11):
        h = i / 100.0
        a = total_active_width * h
        # 🚨 แก้บั๊กเปลี่ยนจาก CS_SUBMERGED เป็นเลข 0.80 โดยตรง
        q_calc = 0.80 * a * math.sqrt(2 * G * delta_h)
        curve_points.append([h, round(q_calc, 2)])

    try:
        calc_result = calculate_gate_openings(q_target, wl_up, wl_down, status_list)
        gate_list = calc_result.get("gates", [])
        capacity_check = {
            "requested_q": calc_result.get("requested_q", q_target),
            "achieved_q": calc_result.get("achieved_q", 0),
            "shortfall_q": calc_result.get("shortfall_q", 0),
            "is_sufficient": calc_result.get("is_sufficient", False),
        }
    except Exception as e:
        print("Backend Error:", e)
        gate_list = []
        capacity_check = {
            "requested_q": q_target,
            "achieved_q": 0,
            "shortfall_q": q_target,
            "is_sufficient": False,
        }

    return {
        "status": "success",
        "results": {
            "target_opening_m": round(target_opening_m, 4),
            "target_opening_cm": round(target_opening_m * 100, 2)
        },
        "chart_data": curve_points,
        "gate_openings": gate_list,
        "capacity_check": capacity_check
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)