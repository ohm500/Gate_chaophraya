import sys
import os
import io
import asyncio
import json
import math
import csv
import pandas as pd
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import socketio
from dotenv import load_dotenv
from supabase import create_client, Client

# --- โหลดค่าการตั้งค่า ---
load_dotenv()
sb_url = os.getenv("SUPABASE_URL")
sb_key = os.getenv("SUPABASE_KEY")

if sb_url and sb_key:
    supabase = create_client(sb_url, sb_key)
else:
    print("⚠️ คำเตือน: ไม่พบการเชื่อมต่อ Supabase")

from gate_calculator import (
    calculate_gate_openings, SILL_ELEV, GATE_WIDTH, MAX_OPENING, G, DEEP_GATES
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI()

# ==============================================================
# 🛡️ ตั้งค่า CORS
# ==============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://chaophraya.rid.go.th",
        "https://chaophraya.rid.go.th",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================
# 🌐 ระบบดึงข้อมูลจากฐานข้อมูล Supabase อัตโนมัติ (C.2 และ C.13)
# ==============================================================
latest_c2_data = {"time": "-", "flow": 0, "updated_at": None}
latest_c13_data = {"time": "-", "level": 0, "updated_at": None} # เพิ่มตัวแปรเก็บระดับน้ำ C.13

async def sync_data_from_supabase_loop():
    print("🤖 [Server] เริ่มระบบซิงค์ข้อมูล C.2 และ C.13 จาก Supabase...")
    await asyncio.sleep(2)
    
    while True:
        try:
            if supabase:
                # 1. ดึงข้อมูลปริมาณน้ำ C.2
                res_c2 = supabase.table("water_history") \
                    .select("record_time, discharge") \
                    .eq("station_name", "C.2") \
                    .not_.is_("discharge", "null") \
                    .order("record_date", desc=True) \
                    .order("record_time", desc=True) \
                    .limit(1).execute()
                
                if res_c2.data and len(res_c2.data) > 0:
                    latest_c2_data["time"] = str(res_c2.data[0]["record_time"])[:5] 
                    latest_c2_data["flow"] = res_c2.data[0]["discharge"]
                    
                # 2. ดึงข้อมูลระดับน้ำ C.13 (เขื่อนเจ้าพระยา)
                res_c13 = supabase.table("water_history") \
                    .select("record_time, water_level") \
                    .eq("station_name", "C.13") \
                    .not_.is_("water_level", "null") \
                    .order("record_date", desc=True) \
                    .order("record_time", desc=True) \
                    .limit(1).execute()
                    
                if res_c13.data and len(res_c13.data) > 0:
                    latest_c13_data["time"] = str(res_c13.data[0]["record_time"])[:5] 
                    latest_c13_data["level"] = res_c13.data[0]["water_level"]
                    
        except Exception as e:
            print(f"❌ [Server] ซิงค์ข้อมูลจาก Supabase ล้มเหลว: {e}")
            
        await asyncio.sleep(60)

# ==============================================================
# ⚙️ ระบบสถานะ Manual / Auto
# ==============================================================
system_mode = {
    "is_manual": False,
    "manual_data": {
        "updated_at": None,
        "c2_time": "-",
        "c2_flow": 0,
        "wl_up": 0,
        "wl_down": 0,
        "flow": 0
    }
}

class ManualDataInput(BaseModel):
    is_manual: bool
    c2_flow: float = 0
    wl_up: float = 0
    wl_down: float = 0
    flow: float = 0
    # เพิ่ม 2 บรรทัดนี้ เพื่อให้รับค่าเวลาจากหน้าแอดมินได้
    updated_at: Optional[str] = None 
    c2_time: Optional[str] = None

# ==============================================================
# 📡 ระบบ Socket.IO (ดึงข้อมูลเขื่อน)
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
SOCKETIO_RETRY_DELAY_SEC = 120  

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
    asyncio.create_task(sync_data_from_supabase_loop())

# ==============================================================
# 🚀 API Endpoints
# ==============================================================

@app.post("/api/v1/system-mode")
def update_system_mode(data: ManualDataInput):
    system_mode["is_manual"] = data.is_manual
    
    if data.is_manual:
        system_mode["manual_data"]["c2_flow"] = data.c2_flow
        system_mode["manual_data"]["wl_up"] = data.wl_up
        system_mode["manual_data"]["wl_down"] = data.wl_down
        system_mode["manual_data"]["flow"] = data.flow
        
        # ถ้าแอดมินส่งเวลามา ให้ใช้เวลานั้น ถ้าไม่ส่งมาให้ใช้เวลาปัจจุบัน
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        system_mode["manual_data"]["updated_at"] = data.updated_at if data.updated_at else current_time
        system_mode["manual_data"]["c2_time"] = data.c2_time if data.c2_time else current_time[11:16] # เอาแค่ HH:MM
        
    return {
        "status": "success", 
        "current_mode": "manual" if data.is_manual else "auto",
        "data": system_mode["manual_data"]
    }

@app.get("/api/v1/realtime")
def get_realtime():
    if system_mode["is_manual"]:
        # ดึงเวลาจากที่แอดมินกรอกมาตัดเอาเฉพาะ HH:MM
        manual_time = system_mode["manual_data"].get("updated_at", "-")
        wl_time_str = manual_time[11:16] if len(manual_time) >= 16 else "-"
        
        return {
            "status": "manual",
            "updated_at": system_mode["manual_data"]["updated_at"],
            "c2_time": system_mode["manual_data"].get("c2_time", "-"), 
            "c2_flow": system_mode["manual_data"]["c2_flow"], 
            "wl_up_time": wl_time_str, # เพิ่มเวลาสำหรับโหมด Manual
            "wl_up": system_mode["manual_data"]["wl_up"],
            "wl_down": system_mode["manual_data"]["wl_down"],
            "flow": system_mode["manual_data"]["flow"],
            "gates": latest_dam_data.get("gates", {}) 
        }
    
    combined_data = latest_dam_data.copy()
    
    # ประกอบข้อมูล C.2
    combined_data["c2_time"] = latest_c2_data["time"]
    combined_data["c2_flow"] = latest_c2_data["flow"]
    
    # ประกอบข้อมูลระดับน้ำเหนือเขื่อน (ดึงจาก C.13 ใน Supabase มาทับของ Socket.IO)
    if latest_c13_data["level"] > 0:
        combined_data["wl_up"] = latest_c13_data["level"]
    combined_data["wl_up_time"] = latest_c13_data["time"]
    
    return combined_data

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
        filename = file.filename.lower()
        
        # 1. ตรวจสอบนามสกุลไฟล์ และแปลงเป็น DataFrame
        if filename.endswith('.csv'):
            # จัดการไฟล์ CSV โดยเฉพาะปัญหาแถว/คอลัมน์ไม่เท่ากัน
            try:
                content_str = contents.decode('utf-8-sig')
            except UnicodeDecodeError:
                content_str = contents.decode('tis-620')
                
            reader = csv.reader(io.StringIO(content_str))
            data_rows = list(reader)
            
            if not data_rows:
                return {"status": "error", "message": "ไฟล์ CSV ว่างเปล่า"}
                
            # เติมช่องว่างให้ทุกแถวมีความยาวเท่ากัน เพื่อไม่ให้ pandas เออเร่อ
            max_cols = max(len(row) for row in data_rows)
            padded_rows = [row + [''] * (max_cols - len(row)) for row in data_rows]
            df = pd.DataFrame(padded_rows)
            
        elif filename.endswith(('.xls', '.xlsx')):
            # จัดการไฟล์ Excel
            df = pd.read_excel(io.BytesIO(contents), header=None)
        else:
            return {"status": "error", "message": "รองรับเฉพาะไฟล์ .csv, .xls, .xlsx เท่านั้น"}

        # 2. ทำความสะอาดค่าว่างให้เป็น NaN
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        
        # 3. ค้นหาบรรทัดหัวตาราง (วันที่, สถานี, ประเภทข้อมูล)
        date_row, station_row, type_row = -1, -1, -1
        for i in range(min(15, len(df))):
            row_str = df.iloc[i].astype(str)
            if row_str.str.contains("วันที่").any() or any(isinstance(x, pd.Timestamp) for x in df.iloc[i]): date_row = i
            if row_str.str.contains("C.2").any() or row_str.str.contains("P.17").any() or row_str.str.contains("สถานี").any(): station_row = i
            if row_str.str.contains("ปริมาณ").any() or row_str.str.contains("ระดับ").any(): type_row = i

        if date_row == -1 or station_row == -1 or type_row == -1:
            return {"status": "error", "message": "โครงสร้างไฟล์ไม่ถูกต้อง ไม่พบหัวตาราง (วันที่, สถานี, หรือ ปริมาณ/ระดับ)"}

        # 4. กระจายข้อมูล (Fill) วันที่และสถานีให้ครบทุกคอลัมน์ (แก้ปัญหาเซลล์ที่ถูก Merge)
        # bfill ช่วยดึงวันที่ ที่มักจะโผล่ไปอยู่คอลัมน์ขวาสุดของ CSV ให้กระจายกลับมาซ้ายสุดด้วย
        df.iloc[date_row] = df.iloc[date_row].ffill().bfill() 
        df.iloc[station_row] = df.iloc[station_row].ffill() 

        records = {}
        data_start_row = type_row + 1

        # 5. วนลูปอ่านข้อมูลทุกคอลัมน์และทุกแถว
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
                
                # แปลงเวลา
                try:
                    hour = int(float(time_raw))
                    time_sql = "23:59:59" if hour == 24 else f"{hour:02d}:00:00"
                except:
                    time_str = str(time_raw).strip()
                    if ":" in time_str:
                        time_sql = time_str if len(time_str) >= 8 else f"{time_str}:00"
                    else:
                        continue
                        
                val = df.iloc[row_idx, col_idx]
                if pd.isna(val) or str(val).strip() in ["", "-", "***", "nan"]: continue 
                
                # แปลงค่าตัวเลข (ตัดลูกน้ำทิ้ง)
                try:
                    val_float = float(str(val).replace(',', ''))
                except ValueError:
                    continue
                
                # แพ็กใส่ Dictionary ป้องกันข้อมูลซ้ำในชั่วโมงเดียวกัน
                key = (sql_date, time_sql, station)
                if key not in records:
                    records[key] = {"record_date": sql_date, "record_time": time_sql, "station_name": station, "water_level": None, "discharge": None}
                    
                if is_level: records[key]["water_level"] = val_float
                elif is_discharge: records[key]["discharge"] = val_float

        final_data = list(records.values())

        # 6. อัปโหลดขึ้น Supabase
        if len(final_data) > 0:
            response = supabase.table("water_history").upsert(
                final_data, 
                on_conflict="record_date,record_time,station_name"
            ).execute()
            return {"status": "success", "message": f"✅ นำเข้าข้อมูลสำเร็จ! อัปเดตเข้าระบบ {len(final_data)} รายการ (จากไฟล์ {filename})"}
        else:
            return {"status": "warning", "message": "⚠️ ไม่พบตัวเลขข้อมูลในไฟล์ที่อัปโหลด"}

    except Exception as e:
        return {"status": "error", "message": f"❌ เกิดข้อผิดพลาด: {str(e)}"}
    
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)