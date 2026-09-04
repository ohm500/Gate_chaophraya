import math

# ==============================================================
# ค่าคงที่ทางชลศาสตร์ (Physical constants)
# ==============================================================
SILL_ELEV = 9.00       # ระดับธรณีประตู (ม.รทก.)
GATE_WIDTH = 12.5      # ความกว้างบานระบาย (ม.)
MAX_OPENING = 5.5      # ระยะยกบานสูงสุด (ม.)
G = 9.81               # ความเร่งโน้มถ่วง (ม./วิ^2)

NUM_GATES = 16
DEEP_GATES = [6, 7, 8, 9]  # บานลึก (บานที่ 7-10)

def calculate_dynamic_cd(y, wl_up):
    H1 = wl_up - SILL_ELEV
    if H1 <= 0 or y <= 0: return 0.65 
    
    ratio = y / H1
    if ratio < 0.1: Cd = 0.72  
    elif ratio > 0.8: Cd = 0.58  
    else: Cd = 0.72 - (0.14 * ((ratio - 0.1) / 0.7)) 
        
    return round(Cd, 3)

def get_max_capacity(wl_up, wl_down, is_deep_gate=False):
    H1 = wl_up - SILL_ELEV
    H2 = wl_down - SILL_ELEV
    delta_h = max(0.001, wl_up - wl_down)

    if H1 <= 0: return 0.0

    Cd_dynamic = calculate_dynamic_cd(MAX_OPENING, wl_up)

    if H2 > MAX_OPENING:
        return 0.80 * GATE_WIDTH * MAX_OPENING * math.sqrt(2 * G * delta_h)
    else:
        eff_h = max(0.001, H1 - (MAX_OPENING / 2))
        return Cd_dynamic * GATE_WIDTH * MAX_OPENING * math.sqrt(2 * G * eff_h)

def get_gate_opening(q_target, wl_up, wl_down, is_deep_gate=False):
    H1 = wl_up - SILL_ELEV
    H2 = wl_down - SILL_ELEV
    delta_h = max(0.001, wl_up - wl_down)

    if H1 <= 0 or q_target <= 0: return 0.0

    low = 0.0
    high = MAX_OPENING
    y = low

    for _ in range(50):
        y = (low + high) / 2
        Cd_dynamic = calculate_dynamic_cd(y, wl_up)

        if H2 > y:
            q_calc = 0.80 * GATE_WIDTH * y * math.sqrt(2 * G * delta_h)
        else:
            eff_h = max(0.001, H1 - (y / 2))
            q_calc = Cd_dynamic * GATE_WIDTH * y * math.sqrt(2 * G * eff_h)

        if q_calc < q_target: low = y
        else: high = y

    return round(y, 4) if y >= 0.01 else 0.0

# ==============================================================
# 🔥 [อัปเกรดใหม่] โมดูลประเมินความปลอดภัยลานคอนกรีต (Jump Control)
# ==============================================================
def check_jump_safety(q_gate, wl_up, wl_down, is_deep_gate=False):
    if q_gate <= 0.01: return True
    
    # ระดับลานรับน้ำ: บานลึก (+5.00), บานปกติ (+4.60)
    apron_elev = 5.00 if is_deep_gate else 4.60
    H = wl_up - apron_elev
    if H <= 0: return True
    
    q = q_gate / GATE_WIDTH
    v1 = 0.95 * math.sqrt(2 * G * H)
    if v1 <= 0: return True
    
    y1 = q / v1
    if y1 <= 0: return True
    
    fr1 = v1 / math.sqrt(G * y1)
    if fr1 <= 1.0: return True # Subcritical (น้ำไหลเรียบ ปลอดภัย)
    
    # คำนวณความสูงน้ำกระโดด
    y2 = (y1 / 2) * (math.sqrt(1 + 8 * (fr1**2)) - 1)
    
    # หักลบ 15% จากการสลายพลังงานของ Baffle Blocks
    req_wl = apron_elev + (y2 * 0.85) 
    
    # ถ้าน้ำท้ายเขื่อน ท่วมมิดระดับที่ต้องการ = ปลอดภัย
    return wl_down >= req_wl

def get_max_safe_q(wl_up, wl_down, is_deep_gate=False):
    # ถ้าน้ำท้ายสููงมาก ปล่อย 500 คิวก็ปลอดภัย ให้คืนค่าไปเลย
    if check_jump_safety(500.0, wl_up, wl_down, is_deep_gate): return 500.0
        
    # Binary Search หาปริมาณน้ำ (Q) ขีดสุดที่โครงสร้างยังทนได้
    low = 0.0
    high = 500.0
    safe_q = 0.0
    
    for _ in range(30):
        mid = (low + high) / 2
        if check_jump_safety(mid, wl_up, wl_down, is_deep_gate):
            safe_q = mid  # ยังปลอดภัยอยู่ ลองเปิดบานเพิ่ม
            low = mid
        else:
            high = mid    # อันตราย! ต้องหรี่บานลง
            
    return safe_q

# ==============================================================
# ตัวช่วย
# ==============================================================
def _empty_result(Q_total):
    gates = [{"หมายเลขบานระบาย": f"บานที่ {i + 1}", "ระยะเปิด (เมตร)": 0.0, "ปริมาณน้ำ (ลบ.ม./วิ)": 0.0} for i in range(NUM_GATES)]
    return {"gates": gates, "requested_q": round(max(0.0, Q_total), 2), "achieved_q": 0.0, "shortfall_q": round(max(0.0, Q_total), 2), "is_sufficient": Q_total <= 0}

# ==============================================================
# ฟังก์ชันหลัก 3: กระจายน้ำด้วยระฆังคว่ำ + Auto-Throttle
# ==============================================================
def calculate_gate_openings(Q_total, wl_up, wl_down, gate_status):
    delta_H = wl_up - wl_down
    if Q_total <= 0 or delta_H <= 0 or wl_up <= SILL_ELEV: return _empty_result(Q_total)

    sample_max_cap = get_max_capacity(wl_up, wl_down)
    if sample_max_cap <= 0: return _empty_result(Q_total)

    if Q_total <= 60: N_target = 2
    elif Q_total <= 120: N_target = 4
    else:
        steps = math.ceil((Q_total - 120) / 25.0)
        N_target = 4 + (steps * 2)

    if N_target > NUM_GATES: N_target = NUM_GATES

    active_indices = [i for i, status in enumerate(gate_status) if status]
    if not active_indices: return _empty_result(Q_total)

    if N_target > len(active_indices): N_target = len(active_indices)

    selected_gates = []
    if N_target == 4 and not any(gate_status[i] for i in DEEP_GATES):
        fallback_1, fallback_2, fallback_3 = [0, 1, 14, 15], [0, 1, 2, 3], [12, 13, 14, 15]
        if sum(1 for i in fallback_1 if gate_status[i]) == 4: selected_gates = fallback_1
        elif sum(1 for i in fallback_2 if gate_status[i]) == 4: selected_gates = fallback_2
        elif sum(1 for i in fallback_3 if gate_status[i]) == 4: selected_gates = fallback_3

    if not selected_gates:
        priority_order = [7, 8, 6, 9, 5, 10, 4, 11, 3, 12, 2, 13, 1, 14, 0, 15]
        for idx in priority_order:
            if gate_status[idx]: selected_gates.append(idx)
            if len(selected_gates) == N_target: break

    center_idx = 7.5
    weights = []
    sigma = max(1.5, len(selected_gates) / 3.0)

    for idx in selected_gates:
        dist = abs(idx - center_idx)
        weights.append(math.exp(-(dist ** 2) / (2 * sigma ** 2)))

    openings, gate_flows = [0.0] * NUM_GATES, [0.0] * NUM_GATES
    remaining_Q = float(Q_total)
    active_pool = selected_gates.copy()
    current_weights = {idx: weights[i] for i, idx in enumerate(selected_gates)}

    max_cap_per_gate = {}
    for idx in selected_gates:
        is_deep = idx in DEEP_GATES
        # 🚨 หัวใจสำคัญ: ระบบ Auto-Throttle
        # 1. ความจุสูงสุดตามบานประตู
        hydraulic_max = get_max_capacity(wl_up, wl_down, is_deep)
        # 2. ความจุสูงสุดที่ไม่ทำให้ลานคอนกรีตพัง (Jump Control)
        safe_max = get_max_safe_q(wl_up, wl_down, is_deep)
        # 3. เลือกค่าที่ต่ำกว่า เพื่อความปลอดภัยสูงสุด
        max_cap_per_gate[idx] = min(hydraulic_max, safe_max)

    # วนลูปกระจายน้ำ (ถ้าน้ำล้น safe_max จะถูกเตะไปให้บานอื่นช่วยระบาย)
    while remaining_Q > 0.01 and len(active_pool) > 0:
        total_w = sum(current_weights[idx] for idx in active_pool)
        if total_w <= 0: break

        round_Q = remaining_Q
        remaining_Q = 0.0

        to_remove = []
        for idx in active_pool:
            share = round_Q * (current_weights[idx] / total_w)
            projected_Q = gate_flows[idx] + share
            max_Q = max_cap_per_gate[idx]

            if projected_Q >= max_Q:
                remaining_Q += (projected_Q - max_Q)
                gate_flows[idx] = max_Q
                to_remove.append(idx)
            else:
                gate_flows[idx] = projected_Q

        for idx in to_remove:
            active_pool.remove(idx)

    for idx in selected_gates:
        is_deep = idx in DEEP_GATES
        openings[idx] = get_gate_opening(gate_flows[idx], wl_up, wl_down, is_deep)

    return {
        "gates": [{"หมายเลขบานระบาย": f"บานที่ {i + 1}", "ระยะเปิด (เมตร)": round(openings[i], 4), "ปริมาณน้ำ (ลบ.ม./วิ)": round(gate_flows[i], 2)} for i in range(NUM_GATES)],
        "requested_q": round(Q_total, 2),
        "achieved_q": round(sum(gate_flows), 2),
        "shortfall_q": max(0.0, round(remaining_Q, 2)),
        "is_sufficient": remaining_Q <= 0.01,
    }