import sys
import os
import io
import asyncio
import json
import math
import pandas as pd
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

# --- นำเข้า FastAPI และไลบรารีที่เกี่ยวข้อง ---
import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import socketio
from dotenv import load_dotenv # <-- นำเข้า dotenv ที่เพิ่งเพิ่มใน requirements.txt
from supabase import create_client, Client # <-- นำเข้า supabase ที่เพิ่งเพิ่มใน requirements.txt

# --- โหลด Environment Variables ---
# โค้ดส่วนนี้จะอ่านค่าจากไฟล์ .env หรือจากค่าที่ตั้งไว้ใน Render
load_dotenv()
sb_url: str = os.getenv("SUPABASE_URL")
sb_key: str = os.getenv("SUPABASE_KEY")

# --- สร้างตัวเชื่อมต่อฐานข้อมูล Supabase ---
if sb_url and sb_key:
    supabase: Client = create_client(sb_url, sb_key)
else:
    print("⚠️ คำเตือน: หา URL หรือ Key ของ Supabase ไม่พบในไฟล์ .env หรือ Environment Variable")

# --- นำเข้าโมดูลคำนวณของนายท่าน ---
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

# ==============================================================
# 🛡️ ตั้งค่าความปลอดภัย (CORS) แบบอัจฉริยะ
# ==============================================================
# อ่านค่า ALLOWED_ORIGINS จาก Environment Variable (ค่าเริ่มต้นคือ *)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://chaophraya.rid.go.th",
        "https://chaophraya.rid.go.th",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
else:
    # กรณีระบุชื่อเว็บ: หั่นด้วยลูกน้ำ (,) และตัดเครื่องหมาย (/) ตัวสุดท้ายออกให้ป้องกัน Error
    origins = [x.strip().rstrip('/') for x in origins_env.split(",") if x.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# ==============================================================
# 📡 ระบบ Socket.IO (เชื่อมต่อเซิร์ฟเวอร์แม่)
# ==============================================================
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
# 🚀 API Endpoints
# ==============================================================

@app.get("/api/v1/realtime")
def get_realtime():
    return latest_dam_data

@app.get("/api/v1/constants")
def get_constants():
    return {
        "sill_elev": SILL_ELEV,
        "gate_width": GATE_WIDTH,
        "max_opening": MAX_OPENING,
        "g": G,
        "cs_free": 0.65,      
        "cs_submerged": 0.80, 
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
    
    a_target = q_target / (0.80 * math.sqrt(2 * G * delta_h)) if delta_h > 0 else 0
    target_opening_m = a_target / total_active_width if total_active_width > 0 else 0

    curve_points = []
    for i in range(0, 11):
        h = i / 100.0
        a = total_active_width * h
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

@app.get("/api/v1/history/{station_name}")
def get_station_history(station_name: str, limit: int = 168):
    try:
        response = supabase.table("water_history") \
            .select("record_date, record_time, water_level, discharge") \
            .eq("station_name", station_name) \
            .order("record_date", desc=False) \
            .order("record_time", desc=False) \
            .limit(limit) \
            .execute()
        
        return {
            "status": "success",
            "station": station_name,
            "total_records": len(response.data),
            "data": response.data
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- ฟังก์ชันแปลงวันที่ ---
def convert_thai_date(thai_date_str):
    if pd.isna(thai_date_str): return None
    if isinstance(thai_date_str, pd.Timestamp): return thai_date_str.strftime("%Y-%m-%d")
        
    months = {
        "มกราคม": "01", "กุมภาพันธ์": "02", "มีนาคม": "03", "เมษายน": "04",
        "พฤษภาคม": "05", "มิถุนายน": "06", "กรกฎาคม": "07", "สิงหาคม": "08",
        "กันยายน": "09", "ตุลาคม": "10", "พฤศจิกายน": "11", "ธันวาคม": "12"
    }
    try:
        date_str = str(thai_date_str).strip()
        if "-" in date_str and len(date_str) >= 10: return date_str[:10]
        parts = date_str.replace("วันที่ ", "").strip().split()
        day = int(parts[0])
        month_name = parts[1]
        year = int(parts[2])
        if year > 2500: year -= 543
        return f"{year}-{months[month_name]}-{day:02d}"
    except:
        return None

@app.post("/api/v1/upload-history")
async def upload_history_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents), header=None)
        
        date_row, station_row, type_row = -1, -1, -1
        for i in range(min(15, len(df))):
            row_str = df.iloc[i].astype(str)
            if row_str.str.contains("วันที่").any() or isinstance(df.iloc[i, 1], pd.Timestamp): date_row = i
            if row_str.str.contains("C.2").any() or row_str.str.contains("P.17").any(): station_row = i
            if row_str.str.contains("ปริมาณ").any() or row_str.str.contains("ระดับ").any(): type_row = i

        if date_row == -1 or station_row == -1 or type_row == -1:
            return {"status": "error", "message": "โครงสร้างไฟล์ Excel ไม่ถูกต้อง ไม่พบหัวตารางที่ต้องการ"}

        df.iloc[date_row] = df.iloc[date_row].ffill() 
        df.iloc[station_row] = df.iloc[station_row].ffill() 

        records = {}
        data_start_row = type_row + 1

        for col_idx in range(1, len(df.columns)):
            date_raw = df.iloc[date_row, col_idx]
            station = str(df.iloc[station_row, col_idx]).strip()
            measure_type_raw = str(df.iloc[type_row, col_idx])
            
            is_level = "ระดับ" in measure_type_raw
            is_discharge = "ปริมาณ" in measure_type_raw
            
            if not (is_level or is_discharge): continue
            
            sql_date = convert_thai_date(date_raw)
            if not sql_date: continue
            
            for row_idx in range(data_start_row, len(df)):
                time_raw = df.iloc[row_idx, 0]
                if pd.isna(time_raw): continue
                try:
                    hour = int(float(time_raw))
                    time_sql = "23:59:59" if hour == 24 else f"{hour:02d}:00:00"
                except:
                    continue
                    
                val = df.iloc[row_idx, col_idx]
                if pd.isna(val) or str(val).strip() == "" or str(val).strip() == "-": continue 
                
                key = (sql_date, time_sql, station)
                if key not in records:
                    records[key] = {"record_date": sql_date, "record_time": time_sql, "station_name": station, "water_level": None, "discharge": None}
                    
                if is_level: records[key]["water_level"] = float(val)
                elif is_discharge: records[key]["discharge"] = float(val)

        final_data = list(records.values())

        if len(final_data) > 0:
            response = supabase.table("water_history").upsert(
                final_data, 
                on_conflict="record_date,record_time,station_name"
            ).execute()
            return {"status": "success", "message": f"✅ อัปโหลดและประมวลผลสำเร็จ! นำเข้าข้อมูล {len(final_data)} แถว"}
        else:
            return {"status": "warning", "message": "⚠️ ไม่พบตัวเลขข้อมูลในไฟล์ที่อัปโหลด"}

    except Exception as e:
        return {"status": "error", "message": f"❌ เกิดข้อผิดพลาด: {str(e)}"}
    
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)