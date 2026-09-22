import sys
import os
import io
import asyncio
import json
import math
import pandas as pd
import csv
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import socketio
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
sb_url = os.getenv("SUPABASE_URL")
sb_key = os.getenv("SUPABASE_KEY")

if sb_url and sb_key:
    supabase = create_client(sb_url, sb_key)
else:
    print("⚠️ คำเตือน: ไม่พบการเชื่อมต่อ Supabase")

from gate_calculator import calculate_gate_openings, SILL_ELEV, GATE_WIDTH, MAX_OPENING, G, DEEP_GATES

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://chaophraya.rid.go.th",
        "https://chaophraya.rid.go.th",
        "http://110.49.150.236",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

latest_c2_data = {"time": "-", "flow": 0, "updated_at": None}
latest_c13_data = {"time": "-", "level": 0, "updated_at": None}

async def sync_data_from_supabase_loop():
    print("🤖 [Server] เริ่มระบบซิงค์ข้อมูล C.2 และ C.13 จาก Supabase...")
    await asyncio.sleep(2) 
    while True:
        try:
            if supabase:
                res_c2 = supabase.table("water_history").select("record_time, discharge").eq("station_name", "C.2").not_.is_("discharge", "null").order("record_date", desc=True).order("record_time", desc=True).limit(1).execute()
                if res_c2.data and len(res_c2.data) > 0:
                    latest_c2_data["time"] = str(res_c2.data[0]["record_time"])[:5] 
                    latest_c2_data["flow"] = res_c2.data[0]["discharge"]
                    
                res_c13 = supabase.table("water_history").select("record_time, water_level").eq("station_name", "C.13").not_.is_("water_level", "null").order("record_date", desc=True).order("record_time", desc=True).limit(1).execute()
                if res_c13.data and len(res_c13.data) > 0:
                    latest_c13_data["time"] = str(res_c13.data[0]["record_time"])[:5] 
                    latest_c13_data["level"] = res_c13.data[0]["water_level"]
        except Exception as e:
            print(f"❌ [Server] ซิงค์ข้อมูลล้มเหลว: {e}")
        await asyncio.sleep(60)

system_mode = {
    "is_manual": False,
    "manual_data": {
        "updated_at": None, "c2_time": "-", "c2_flow": 0, "wl_up": 0, "wl_down": 0, "flow": 0
    }
}

class ManualDataInput(BaseModel):
    is_manual: bool
    c2_flow: float = 0
    wl_up: float = 0
    wl_down: float = 0
    flow: float = 0
    updated_at: Optional[str] = None 
    c2_time: Optional[str] = None

# ==============================================================
# 📢 ระบบจัดการข้อความอักษรวิ่ง (Marquee) - เวอร์ชันอัปเกรด
# ==============================================================
marquee_state = {
    "is_custom": False,
    "text": "",
    "color": "#facc15" # ค่าเริ่มต้นสีเหลือง
}

class MarqueeInput(BaseModel):
    is_custom: bool
    text: str = ""
    color: str = "#facc15"

@app.post("/api/v1/marquee")
def update_marquee(data: MarqueeInput):
    marquee_state["is_custom"] = data.is_custom
    marquee_state["text"] = data.text
    marquee_state["color"] = data.color
    return {"status": "success", "message": "อัปเดตข้อความอักษรวิ่งสำเร็จ!"}

sio = socketio.AsyncClient()
latest_dam_data = {"status": "waiting", "updated_at": None, "wl_up": 15.86, "wl_down": 0, "flow": 0, "gates": {}}
SOCKETIO_URL = "http://110.49.150.236"
SOCKETIO_RETRY_DELAY_SEC = 120  

@sio.event
async def connect():
    print("✅ [Socket.IO] เชื่อมต่อสำเร็จ!")
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
                pass
        else:
            return
        await asyncio.sleep(15)

@sio.on('server-report')
async def on_server_report(data):
    global latest_dam_data
    try:
        if 'DATA' in data:
            parsed_data = json.loads(data['DATA']) if isinstance(data['DATA'], str) else data['DATA']
            if isinstance(parsed_data, list) and len(parsed_data) > 0:
                latest_record = parsed_data[-1]
                latest_dam_data["status"] = "connected"
                if "LASTUPDATE" in latest_record: latest_dam_data["updated_at"] = latest_record["LASTUPDATE"]
                if latest_record.get("WL_UP") is not None: latest_dam_data["wl_up"] = float(latest_record.get("WL_UP"))
                if latest_record.get("WL_DOWN") is not None: latest_dam_data["wl_down"] = float(latest_record.get("WL_DOWN"))
                if latest_record.get("FLOW") is not None: latest_dam_data["flow"] = float(latest_record.get("FLOW"))
                for i in range(1, 17):
                    if f"G{i}" in latest_record: latest_dam_data["gates"][f"GATE{i:02d}"] = float(latest_record.get(f"G{i}") or 0)
    except Exception as e:
        pass

async def start_socketio():
    while True:
        try:
            latest_dam_data["status"] = "waiting"
            await sio.connect(SOCKETIO_URL, transports=['websocket'])
            await sio.wait()  
        except:
            pass
        await asyncio.sleep(SOCKETIO_RETRY_DELAY_SEC)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_socketio())
    asyncio.create_task(sync_data_from_supabase_loop())

@app.post("/api/v1/system-mode")
def update_system_mode(data: ManualDataInput):
    system_mode["is_manual"] = data.is_manual
    if data.is_manual:
        system_mode["manual_data"].update({"c2_flow": data.c2_flow, "wl_up": data.wl_up, "wl_down": data.wl_down, "flow": data.flow})
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        system_mode["manual_data"]["updated_at"] = data.updated_at if data.updated_at else current_time
        system_mode["manual_data"]["c2_time"] = data.c2_time if data.c2_time else current_time[11:16]
    return {"status": "success", "current_mode": "manual" if data.is_manual else "auto", "data": system_mode["manual_data"]}

@app.get("/api/v1/realtime")
def get_realtime():
    if system_mode["is_manual"]:
        manual_time = system_mode["manual_data"].get("updated_at", "-")
        wl_time_str = manual_time[11:16] if len(manual_time) >= 16 else "-"
        return {
            "status": "manual", "updated_at": system_mode["manual_data"]["updated_at"],
            "c2_time": system_mode["manual_data"].get("c2_time", "-"), "c2_flow": system_mode["manual_data"]["c2_flow"], 
            "wl_up_time": wl_time_str, "wl_up": system_mode["manual_data"]["wl_up"],
            "wl_down": system_mode["manual_data"]["wl_down"], "flow": system_mode["manual_data"]["flow"],
            "gates": latest_dam_data.get("gates", {}),
            "marquee_is_custom": marquee_state["is_custom"], # ✨ เพิ่มตัวแปรเช็กสถานะ
            "marquee_text": marquee_state["text"],
            "marquee_color": marquee_state["color"] # ✨ เพิ่มตัวแปรสี
        }
    
    combined_data = latest_dam_data.copy()
    combined_data["c2_time"] = latest_c2_data["time"]
    combined_data["c2_flow"] = latest_c2_data["flow"]
    if latest_c13_data["level"] > 0: combined_data["wl_up"] = latest_c13_data["level"]
    combined_data["wl_up_time"] = latest_c13_data["time"]
    
    combined_data["marquee_is_custom"] = marquee_state["is_custom"]
    combined_data["marquee_text"] = marquee_state["text"]
    combined_data["marquee_color"] = marquee_state["color"]
    
    return combined_data

@app.get("/api/v1/constants")
def get_constants():
    return {"sill_elev": SILL_ELEV, "gate_width": GATE_WIDTH, "max_opening": MAX_OPENING, "g": G, "cs_free": 0.65, "cs_submerged": 0.80, "deep_gates": DEEP_GATES}

@app.get("/api/v1/calculate-flow")
def calculate_flow(q_target: float, delta_h: float, gate_status: str, wl_up: float = 15.86, wl_down: Optional[float] = None):
    if wl_down is None: wl_down = wl_up - delta_h
    status_list = [s == '1' for s in gate_status.split(',')]
    active_count = sum(status_list)

    if active_count == 0 or q_target <= 0 or delta_h <= 0:
        return {"status": "success", "results": {"target_opening_m": 0, "target_opening_cm": 0}, "chart_data": [[i/100.0, 0] for i in range(11)], "gate_openings": [{"หมายเลขบานระบาย": f"บานที่ {i+1}", "ระยะเปิด (เมตร)": 0.0, "ปริมาณน้ำ (ลบ.ม./วิ)": 0.0} for i in range(16)], "capacity_check": {"requested_q": q_target, "achieved_q": 0, "shortfall_q": max(0, q_target), "is_sufficient": q_target <= 0}}

    total_active_width = active_count * GATE_WIDTH
    target_opening_m = (q_target / (0.80 * math.sqrt(2 * G * delta_h))) / total_active_width if total_active_width > 0 and delta_h > 0 else 0

    try:
        calc_result = calculate_gate_openings(q_target, wl_up, wl_down, status_list)
        gate_list = calc_result.get("gates", [])
        capacity_check = {"requested_q": calc_result.get("requested_q", q_target), "achieved_q": calc_result.get("achieved_q", 0), "shortfall_q": calc_result.get("shortfall_q", 0), "is_sufficient": calc_result.get("is_sufficient", False)}
    except Exception:
        gate_list, capacity_check = [], {"requested_q": q_target, "achieved_q": 0, "shortfall_q": q_target, "is_sufficient": False}

    return {"status": "success", "results": {"target_opening_m": round(target_opening_m, 4), "target_opening_cm": round(target_opening_m * 100, 2)}, "chart_data": [[i/100.0, round(0.80*(total_active_width*(i/100.0))*math.sqrt(2*G*delta_h), 2)] for i in range(11)], "gate_openings": gate_list, "capacity_check": capacity_check}

def convert_thai_date(thai_date_str):
    if pd.isna(thai_date_str): return None
    if isinstance(thai_date_str, pd.Timestamp): return thai_date_str.strftime("%Y-%m-%d")
    months = {"มกราคม": "01", "กุมภาพันธ์": "02", "มีนาคม": "03", "เมษายน": "04", "พฤษภาคม": "05", "มิถุนายน": "06", "กรกฎาคม": "07", "สิงหาคม": "08", "กันยายน": "09", "ตุลาคม": "10", "พฤศจิกายน": "11", "ธันวาคม": "12"}
    try:
        date_str = str(thai_date_str).strip()
        if "-" in date_str and len(date_str) >= 10: return date_str[:10]
        parts = date_str.replace("วันที่ ", "").strip().split()
        year = int(parts[2])
        return f"{year - 543 if year > 2500 else year}-{months[parts[1]]}-{int(parts[0]):02d}"
    except: return None

@app.post("/api/v1/upload-history")
async def upload_history_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            try: content_str = contents.decode('utf-8-sig')
            except: content_str = contents.decode('tis-620')
            data_rows = list(csv.reader(io.StringIO(content_str)))
            if not data_rows: return {"status": "error", "message": "ไฟล์ CSV ว่างเปล่า"}
            max_cols = max(len(row) for row in data_rows)
            df = pd.DataFrame([row + [''] * (max_cols - len(row)) for row in data_rows])
        elif filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(contents), header=None)
        else: return {"status": "error", "message": "รองรับเฉพาะไฟล์ .csv, .xls, .xlsx เท่านั้น"}

        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        date_row = station_row = type_row = -1
        for i in range(min(15, len(df))):
            row_str = df.iloc[i].astype(str)
            if row_str.str.contains("วันที่").any() or any(isinstance(x, pd.Timestamp) for x in df.iloc[i]): date_row = i
            if row_str.str.contains("C.2").any() or row_str.str.contains("P.17").any() or row_str.str.contains("สถานี").any(): station_row = i
            if row_str.str.contains("ปริมาณ").any() or row_str.str.contains("ระดับ").any(): type_row = i

        if -1 in [date_row, station_row, type_row]: return {"status": "error", "message": "โครงสร้างไฟล์ไม่ถูกต้อง"}
        df.iloc[date_row] = df.iloc[date_row].ffill().bfill() 
        df.iloc[station_row] = df.iloc[station_row].ffill() 

        records = {}
        for col_idx in range(1, len(df.columns)):
            date_raw, station, measure_type_raw = df.iloc[date_row, col_idx], str(df.iloc[station_row, col_idx]).strip(), str(df.iloc[type_row, col_idx])
            is_level, is_discharge = "ระดับ" in measure_type_raw, "ปริมาณ" in measure_type_raw
            if not (is_level or is_discharge): continue
            sql_date = convert_thai_date(date_raw)
            if not sql_date: continue
            
            for row_idx in range(type_row + 1, len(df)):
                time_raw, val = df.iloc[row_idx, 0], df.iloc[row_idx, col_idx]
                if pd.isna(time_raw) or pd.isna(val) or str(val).strip() in ["", "-", "***", "nan"]: continue
                try: time_sql = "23:59:59" if int(float(time_raw)) == 24 else f"{int(float(time_raw)):02d}:00:00"
                except:
                    time_str = str(time_raw).strip()
                    if ":" in time_str: time_sql = time_str if len(time_str) >= 8 else f"{time_str}:00"
                    else: continue
                try: val_float = float(str(val).replace(',', ''))
                except: continue
                
                key = (sql_date, time_sql, station)
                if key not in records: records[key] = {"record_date": sql_date, "record_time": time_sql, "station_name": station, "water_level": None, "discharge": None}
                if is_level: records[key]["water_level"] = val_float
                elif is_discharge: records[key]["discharge"] = val_float

        final_data = list(records.values())
        if final_data:
            supabase.table("water_history").upsert(final_data, on_conflict="record_date,record_time,station_name").execute()
            return {"status": "success", "message": f"✅ นำเข้าข้อมูลสำเร็จ! อัปเดตเข้าระบบ {len(final_data)} รายการ"}
        return {"status": "warning", "message": "⚠️ ไม่พบตัวเลขข้อมูลในไฟล์ที่อัปโหลด"}
    except Exception as e: return {"status": "error", "message": str(e)}

# ==============================================================
# 📈 API ดึงข้อมูลประวัติสำหรับวาดกราฟ (ที่หายไป)
# ==============================================================
@app.get("/api/v1/history/{station_name}")
def get_history(station_name: str, limit: int = 168):
    try:
        if not supabase:
            return {"status": "error", "message": "ไม่พบการเชื่อมต่อฐานข้อมูล"}
            
        res = supabase.table("water_history") \
            .select("*") \
            .eq("station_name", station_name) \
            .order("record_date", desc=True) \
            .order("record_time", desc=True) \
            .limit(limit).execute()
        
        # กลับด้านข้อมูล (Reverse) เพื่อให้กราฟเรียงจากอดีต -> ปัจจุบัน (ซ้ายไปขวา)
        data = res.data[::-1] if res.data else []
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}    

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)