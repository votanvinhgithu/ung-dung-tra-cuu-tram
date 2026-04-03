import streamlit as st
import pandas as pd
import io
import os
from datetime import datetime, timedelta
import re

# --- CẤU HÌNH GIAO DIỆN WEB ---
st.set_page_config(page_title="Hệ Thống Tra Cứu Hợp Đồng", page_icon="📡", layout="wide")

st.title("📡 Tra Cứu Hợp Đồng & Quản Lý Thanh Toán")

TARGET_COLUMNS = [
    "mã trạm", "Q/H", "long thuê", "lat thuê", "Địa chỉ", 
    "Viettel", "Vina", "Mobi", 
    "Ngày ký HĐ Chủ nhà_trên HĐ", "Ngày hết hạn HĐ", 
    "Chủ nhà + SĐT", "giá thuê chủ nhà", "Giá Viettel Thuê", 
    "Giá MB thuê", "Giá Vina thuê", "chu kỳ thanh toán cho chủ nhà", 
    "Số HĐ với chủ nhà", "Số TK chủ nhà", "Chủ tài khoản", "Tên Ngân Hàng"
]

EXTRA_PAY_COLS = [
    "Ngày tới hạn TT trong tháng",
    "Ngày TT kỳ trước",
    "Ngày đến hạn TT kỳ tiếp theo",
    "Số tiền cần thanh toán"
]

DISPLAY_COLUMNS = TARGET_COLUMNS + EXTRA_PAY_COLS

def normalize_str(s):
    return str(s).strip().lower()

def display_error(msg):
    st.markdown(f'<p style="color:red; font-size:1.5em; font-weight:bold;">❌ {msg}</p>', unsafe_allow_html=True)

def validate_month_year(val):
    val = str(val).strip()
    if not val: return False
    try:
        parts = val.split('/')
        if len(parts) == 2:
            m = int(parts[0])
            y = int(parts[1])
            if 1 <= m <= 12 and y > 0: return True
    except:
        pass
    return False

def validate_month_year_or_year(val):
    val = str(val).strip()
    if not val: return False
    
    if '-' in val:
        parts = val.split('-')
        if len(parts) == 2:
            try:
                sm, sy = [int(p) for p in parts[0].strip().split('/')]
                em, ey = [int(p) for p in parts[1].strip().split('/')]
                if 1 <= sm <= 12 and 1 <= em <= 12 and sy > 0 and ey > 0:
                    if ey > sy or (ey == sy and em >= sm):
                        return True
            except:
                pass
        return False

    try:
        parts = val.split('/')
        if len(parts) == 1:
            y = int(parts[0])
            if y > 0: return True
        elif len(parts) == 2:
            m = int(parts[0])
            y = int(parts[1])
            if 1 <= m <= 12 and y > 0: return True
    except:
        pass
    return False

def validate_input_date(val):
    val = val.strip()
    if not val:
        return True, ""
    try:
        datetime.strptime(val, '%m/%d/%Y')
        return True, ""
    except ValueError:
        parts = val.split('/')
        if len(parts) >= 2:
            thang = parts[0]
            if len(thang) == 1: thang = "0" + thang
            return False, f"Bạn đã nhập sai ngày, tháng {thang} không có ngày {val}, vui lòng nhập lại đúng ngày để hệ thống hiển thị kết quả, xin cám ơn"
        return False, f"Bạn đã nhập sai định dạng ngày, {val} không đúng chuẩn (MM/DD/YYYY), vui lòng nhập lại đúng định dạng để hệ thống hiển thị kết quả, xin cám ơn"

def enrich_payment_data(df_main, df_pay, target_month, target_year):
    pay_dict = {}
    ma_tram_col = None
    amount_col = None
    
    # Ưu tiên lấy cột mã trạm
    for c in df_pay.columns:
        c_low = str(c).strip().lower()
        if "mã trạm" in c_low or "mã" in c_low:
            if ma_tram_col is None: ma_tram_col = c
            
    # Ưu tiên tìm cột "thuê/tháng", "số tiền thuê/tháng" ở sheet 2
    for c in df_pay.columns:
        c_low = str(c).strip().lower()
        if "thuê/tháng" in c_low or "số tiền thuê" in c_low or "tiền thuê/tháng" in c_low:
            amount_col = c
            break
    # Nếu không có "thuê/tháng" thì rớt xuống tìm cột "số tiền"
    if not amount_col:
        for c in df_pay.columns:
            c_low = str(c).strip().lower()
            if "số tiền thanh toán" in c_low or "số tiền" in c_low:
                amount_col = c
                break
                
    if not ma_tram_col: return df_main # Bỏ qua nếu sheet 2 không có cột mã trạm
    
    date_cols = [c for c in df_pay.columns if c != ma_tram_col and c != amount_col]
    
    for _, row in df_pay.iterrows():
        ma_tram = str(row[ma_tram_col]).strip().lower() if pd.notna(row[ma_tram_col]) else ""
        if not ma_tram: continue
        
        # Bốc KHỚP nguyên giá trị của Cột tiền tại Sheet 2 (số tiền thuê/tháng)
        amount = row[amount_col] if amount_col and pd.notna(row[amount_col]) else 0
        try:
            amount_val = float(str(amount).replace(',', '').replace(' ', ''))
        except:
            amount_val = 0.0
            
        dates = []
        for c in date_cols:
            val = row[c]
            if pd.notna(val):
                if isinstance(val, pd.Timestamp) or isinstance(val, datetime):
                    dates.append(pd.to_datetime(val))
                else:
                    try:
                        d = pd.to_datetime(val)
                        dates.append(d)
                    except:
                        pass
        dates.sort()
        
        due_this_month = [d for d in dates if d.year == target_year and d.month == target_month]
        due_date = due_this_month[0] if due_this_month else None
        
        prev_date = None
        next_date = None
        
        # Mốc ngày 1 của tháng hiện tại
        target_date_ref = datetime(target_year, target_month, 1)
        past_dates = [d for d in dates if d < target_date_ref]
        future_dates = [d for d in dates if d >= target_date_ref and d not in due_this_month]

        if due_date:
            idx = dates.index(due_date)
            if idx > 0: prev_date = dates[idx-1]
            if idx < len(dates) - 1: next_date = dates[idx+1]
        else:
            if past_dates: prev_date = max(past_dates)
            if future_dates: next_date = min(future_dates)
            
        def fmt(d): return d.strftime('%m/%d/%Y') if d else "-"
        
        pay_dict[ma_tram] = {
            "Ngày tới hạn TT trong tháng": fmt(due_date) if due_date else "Không có",
            "Ngày TT kỳ trước": fmt(prev_date),
            "Ngày đến hạn TT kỳ tiếp theo": fmt(next_date),
            "__raw_amount__": amount_val,
            "__is_due_this_month__": bool(due_date)
        }
        
    df_res = df_main.copy()
    new_cols = {
        "Ngày tới hạn TT trong tháng": [],
        "Ngày TT kỳ trước": [],
        "Ngày đến hạn TT kỳ tiếp theo": [],
        "Số tiền cần thanh toán": [],
        "giá thuê chủ nhà": [], # Cột này sẽ đè Cột có sẵn trên Sheet 1
        "__raw_amount__": [],
        "__is_due_this_month__": []
    }
    
    for _, row in df_res.iterrows():
        ma = str(row.get("mã trạm", "")).strip().lower()
        info = pay_dict.get(ma, {})
        
        # Tiền gốc từ cột "số tiền thuê/tháng" ở Sheet 2
        base_monthly = info.get("__raw_amount__", 0.0)
        
        # Nếu Sheet 2 không có / không tồn tại trạm thì lấy dự phòng ở Sheet 1
        if base_monthly == 0.0:
            raw_price_s1 = str(row.get("giá thuê chủ nhà", "0"))
            try:
                price_digits = re.sub(r'[^\d.]', '', raw_price_s1.replace(',', '.'))
                base_monthly = float(price_digits) if price_digits else 0.0
            except:
                pass
                
        # FORMAT số tiền một tháng CẬP NHẬT lên TAB 1 và TAB 2
        formatted_monthly = f"{base_monthly:,.0f}" if base_monthly > 0 else "-"
        
        # Đọc chu kỳ thanh toán ở Sheet 1 (cột này có sẵn trong dataframe df_res)
        raw_cycle = str(row.get("chu kỳ thanh toán cho chủ nhà", "1")).strip().lower()
        
        # Xử lý ngoại lệ "năm" -> nhân thêm hệ số 12
        multiplier = 12.0 if "năm" in raw_cycle else 1.0
        
        try:
            # Rút trích con số trong chuỗi "6 tháng", "1 năm"
            cycle_digits = re.sub(r'[^\d.]', '', raw_cycle.replace(',', '.'))
            cycle_val = float(cycle_digits) if cycle_digits else 1.0
            if cycle_val == 0.0: cycle_val = 1.0 # Tránh chia/nhân 0
        except:
            cycle_val = 1.0
            
        real_cycle = cycle_val * multiplier
        
        # TRỌNG TÂM: SỐ TIỀN THANH TOÁN (1 Kỳ) = Tiền 1 tháng * Số tháng chu kỳ
        calc_amount = base_monthly * real_cycle
        
        if calc_amount > 0:
            formatted_amount = f"{calc_amount:,.0f}"
        else:
            formatted_amount = "-"
            
        new_cols["Ngày tới hạn TT trong tháng"].append(info.get("Ngày tới hạn TT trong tháng", "-"))
        new_cols["Ngày TT kỳ trước"].append(info.get("Ngày TT kỳ trước", "-"))
        new_cols["Ngày đến hạn TT kỳ tiếp theo"].append(info.get("Ngày đến hạn TT kỳ tiếp theo", "-"))
        
        # Ghi đè vào kết quả hiển thị
        new_cols["giá thuê chủ nhà"].append(formatted_monthly)
        new_cols["Số tiền cần thanh toán"].append(formatted_amount)
        new_cols["__raw_amount__"].append(calc_amount)
        new_cols["__is_due_this_month__"].append(info.get("__is_due_this_month__", False))
        
    for k, v in new_cols.items():
        df_res[k] = v
        
    return df_res

# --- HÀM XỬ LÝ DOANH THU NHÀ MẠNG (TAB 3) ---
# (Đã tắt @st.cache_data để file Excel vừa lưu sửa là Cập nhật lên Web ngay lập tức, không bị kẹt bộ nhớ tạm)
def load_revenue_data_v2(file_source, target_month_str):
    try:
        parts = str(target_month_str).strip().split('/')
        if len(parts) == 2:
            try:
                target_month = int(parts[0])
                target_year  = int(parts[1])
            except:
                target_month = datetime.now().month
                target_year = datetime.now().year
        else:
            target_month = datetime.now().month
            target_year = datetime.now().year
            
        if hasattr(file_source, 'seek'):
            file_source.seek(0)
        xl = pd.ExcelFile(file_source)
        sheets = xl.sheet_names
        
        def process_provider(provider_keyword):
            target_sheet = next((s for s in sheets if provider_keyword.lower() in s.lower()), None)
            if not target_sheet: return None
            
            df = pd.read_excel(file_source, sheet_name=target_sheet)
            if df.empty: return None
            
            ma_col = df.columns[0]
            for c in df.columns:
                if "mã trạm" in str(c).lower() or "mã" in str(c).lower():
                    ma_col = c; break
                    
            # 1. TÌM CỘT CUỐI CÙNG (bỏ qua Ghi chú và các cột Unnamed trống)
            valid_cols_for_last = [c for c in df.columns if "ghi chú" not in str(c).lower() and "note" not in str(c).lower()]
            filtered_cols = []
            for c in valid_cols_for_last:
                if str(c).lower().startswith("unnamed"):
                    # Giữ lại Unnamed nếu có chứa dữ liệu thực sự
                    if not df[c].replace('', pd.NA).dropna().empty:
                        filtered_cols.append(c)
                else:
                    filtered_cols.append(c)
            last_col_idx = filtered_cols[-1] if filtered_cols else df.columns[-1]

            # 2. TÌM CỘT TRẢ/THÁNG (Giá thuê) BẰNG TỪ KHÓA ƯU TIÊN
            monthly_col = None
            kw_list = ["trả/tháng", "thuê/tháng", "giá thuê", "đơn giá", "mức cước", "số tiền", "cước", "giá"]
            for kw in kw_list:
                for c in df.columns:
                    c_str = str(c).lower()
                    if kw in c_str and (c != last_col_idx) and (c != ma_col):
                        monthly_col = c
                        break
                if monthly_col: break
                
            if not monthly_col:
                monthly_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            date_cols = [c for c in df.columns if c != ma_col and c != monthly_col and c != last_col_idx]
            
            import re
            records = []
            
            for _, row in df.iterrows():
                ma_tram = str(row[ma_col]).strip().lower() if pd.notna(row[ma_col]) else ""
                if not ma_tram or ma_tram == 'nan': continue
                
                # Scan tìm tất cả các ngày (từ N+1 đến N+x)
                dates = []
                for c in date_cols:
                    val = row[c]
                    if pd.notna(val):
                        if isinstance(val, (pd.Timestamp, datetime)):
                            dates.append(pd.to_datetime(val))
                        else:
                            try:
                                d = pd.to_datetime(val)
                                dates.append(d)
                            except:
                                pass
                
                # Lọc xem trạm này CÓ kỳ thanh toán trùng với TRONG THÁNG đang tra cứu không
                due_this_month = [d for d in dates if d.year == target_year and d.month == target_month]
                if not due_this_month:
                    continue # BỎ QUA nếu KHÔNG CÓ KỲ NÀO rơi vào tháng này
                    
                due_date = due_this_month[0]
                
                # Quét tìm kỳ tiếp theo (Ngày CÓ NẰM TRONG MẢNG nhưng LỚN HƠN due_date)
                future_dates = [d for d in dates if d > due_date]
                next_due_date_str = min(future_dates).strftime('%m/%d/%Y') if future_dates else "Không có"
                
                # Hàm xử lý trị số thông minh (Sửa lỗi Pandas tự thêm .0 vào cuối số nguyên)
                def parse_vn_money(val):
                    if pd.isna(val): return 0.0
                    # Nếu file Excel đã format dạng số chuẩn, giữ nguyên
                    if isinstance(val, (int, float)): return float(val)
                    
                    s = str(val).strip()
                    # Sửa lỗi Pandas đọc 5000000 thành "5000000.0"
                    if s.endswith('.0'): 
                        s = s[:-2]
                    # Loại bỏ dấu phân cách (dấu chấm, phẩy), chỉ giữ lại số
                    digits = re.sub(r'\D', '', s)
                    return float(digits) if digits else 0.0

                monthly_val = parse_vn_money(row[monthly_col])
                payment_val = parse_vn_money(row[last_col_idx])
                    
                records.append({
                    'Mã trạm': str(row[ma_col]).strip() if pd.notna(row[ma_col]) else "",
                    f'Số tiền {provider_keyword} trả/tháng': f"{monthly_val:,.0f}" if monthly_val > 0 else "-",
                    f'Kỳ {provider_keyword} thanh toán': due_date.strftime('%m/%d/%Y'),
                    'Ngày đến kỳ thanh toán tiếp theo': next_due_date_str,
                    f'Số tiền {provider_keyword} thanh toán': f"{payment_val:,.0f}" if payment_val > 0 else "-",
                    '__raw_payment__': payment_val
                })
                
            df_clean = pd.DataFrame(records)
            
            # Sắp xếp theo ngày tăng dần từ đầu tháng tới cuối tháng
            if not df_clean.empty:
                df_clean = df_clean.sort_values(
                    by=f'Kỳ {provider_keyword} thanh toán',
                    key=lambda col: pd.to_datetime(col, format='%m/%d/%Y', errors='coerce')
                )
                
            return df_clean
            
        return process_provider("Viettel"), process_provider("Vina"), process_provider("Mobi")
    except Exception as e:
        return None, None, None

# --- HÀM TỔNG HỢP LỢI NHUẬN (TAB 4) ---
def get_profit_report_data(file_source, time_input_str, df_source):
    time_input_str = str(time_input_str).strip()
    
    target_months_years = []
    
    if '-' in time_input_str:
        p1, p2 = time_input_str.split('-')
        sm, sy = [int(x) for x in p1.strip().split('/')]
        em, ey = [int(x) for x in p2.strip().split('/')]
        
        curr_m, curr_y = sm, sy
        while (curr_y < ey) or (curr_y == ey and curr_m <= em):
            target_months_years.append((curr_m, curr_y))
            curr_m += 1
            if curr_m > 12:
                curr_m = 1
                curr_y += 1
    else:
        parts = time_input_str.split('/')
        if len(parts) == 2:
            try:
                target_months_years = [(int(parts[0]), int(parts[1]))]
            except:
                target_months_years = [(datetime.now().month, datetime.now().year)]
        else:
            try:
                target_year = int(time_input_str)
                target_months_years = [(m, target_year) for m in range(1, 13)]
            except:
                target_year = datetime.now().year
                target_months_years = [(m, target_year) for m in range(1, 13)]
    
    # 1. TỔNG TIỀN CHỦ NHÀ
    chu_nha_totals = []
    for m, y in target_months_years:
        target_m_str = f"{m:02d}/{y}"
        df_pay = load_data_and_enrich_v3(file_source, target_m_str)
        if df_pay is not None and not df_pay.empty:
            df_pay_m = df_pay[df_pay["__is_due_this_month__"] == True]
            tot = df_pay_m["__raw_amount__"].sum()
        else:
            tot = 0.0
        chu_nha_totals.append(tot)
        
    # 2. TỔNG DOANH THU CÁC NHÀ MẠNG
    xl = pd.ExcelFile(file_source)
    sheets = xl.sheet_names
    
    def sum_provider_for_year(provider_keyword):
        monthly_sums = [0.0] * len(target_months_years)
        target_sheet = next((s for s in sheets if provider_keyword.lower() in s.lower()), None)
        if not target_sheet: return monthly_sums
        
        if hasattr(file_source, 'seek'):
            file_source.seek(0)
        df = pd.read_excel(file_source, sheet_name=target_sheet)
        if df.empty: return monthly_sums
        
        ma_col = df.columns[0]
        valid_cols = [c for c in df.columns if "ghi chú" not in str(c).lower() and "note" not in str(c).lower()]
        filtered = []
        for c in valid_cols:
            if str(c).lower().startswith("unnamed"):
                if not df[c].replace('', pd.NA).dropna().empty: filtered.append(c)
            else:
                filtered.append(c)
        last_col = filtered[-1] if filtered else df.columns[-1]

        m_col = None
        kw_list = ["trả/tháng", "thuê/tháng", "giá thuê", "đơn giá", "mức cước", "số tiền", "cước", "giá"]
        for kw in kw_list:
            for c in df.columns:
                c_str = str(c).lower()
                if kw in c_str and (c != last_col) and (c != ma_col):
                    m_col = c; break
            if m_col: break
        if not m_col: m_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        date_cols = [c for c in df.columns if c != ma_col and c != m_col and c != last_col]
        
        def parse_num(val):
            if pd.isna(val): return 0.0
            if isinstance(val, (int, float)): return float(val)
            s = str(val).strip()
            if s.endswith('.0'): s = s[:-2]
            digits = re.sub(r'\D', '', s)
            return float(digits) if digits else 0.0
            
        for _, row in df.iterrows():
            payment_val = parse_num(row[last_col])
            if payment_val == 0: continue
            
            dates = []
            for c in date_cols:
                val = row[c]
                if pd.notna(val):
                    if isinstance(val, (pd.Timestamp, datetime)): dates.append(pd.to_datetime(val))
                    else:
                        try: dates.append(pd.to_datetime(val))
                        except: pass
            
            for d in dates:
                for idx, (tm, ty) in enumerate(target_months_years):
                    if d.month == tm and d.year == ty:
                        monthly_sums[idx] += payment_val
                    
        return monthly_sums

    viettel_totals = sum_provider_for_year("Viettel")
    vina_totals = sum_provider_for_year("Vina")
    mobi_totals = sum_provider_for_year("Mobi")
    
    # 3. BUILD DATAFRAME TỔNG
    records = []
    for i, (m, y) in enumerate(target_months_years):
        sum_rev = viettel_totals[i] + vina_totals[i] + mobi_totals[i]
        chu_nha = chu_nha_totals[i]
        profit = (sum_rev / 1.1) - chu_nha
        
        records.append({
            "Tháng": f"{m:02d}/{y}",
            "Doanh thu Viettel": viettel_totals[i],
            "Doanh thu Vina": vina_totals[i],
            "Doanh thu Mobi": mobi_totals[i],
            "Tổng Doanh Thu": sum_rev,
            "Tiền Chủ Nhà": chu_nha,
            "Lợi nhuận": profit
        })
        
    return pd.DataFrame(records)


# --- HÀM LẤY DANH SÁCH MÃ TRẠM "CÁ NHÂN" TỪ CÁC SHEET ---
def get_ca_nhan_ma_trams(file_source):
    """
    Quét sheet 2, 3, 4, 5 (index 1..4). Với mỗi sheet, 
    tìm những hàng có cột đầu tiên chứa 'cá nhân',
    lấy giá trị cột 2 (mã trạm) rồi trả về set mã trạm loại trừ.
    """
    excluded = set()
    try:
        if hasattr(file_source, 'seek'):
            file_source.seek(0)
        xl = pd.ExcelFile(file_source)
        sheets = xl.sheet_names
        
        for sheet_idx in [1, 2, 3, 4]:  # sheet 2, 3, 4, 5
            if sheet_idx >= len(sheets):
                continue
            sheet_name = sheets[sheet_idx]
            if hasattr(file_source, 'seek'):
                file_source.seek(0)
            df = pd.read_excel(file_source, sheet_name=sheet_name)
            if df.empty or len(df.columns) < 2:
                continue
            col0 = df.columns[0]   # Stt
            col1 = df.columns[1]   # Mã trạm
            ca_nhan_rows = df[df[col0].astype(str).str.strip().str.lower() == 'cá nhân']
            for _, row in ca_nhan_rows.iterrows():
                ma = str(row[col1]).strip()
                if ma and ma.lower() not in ('nan', ''):
                    # Normalize: bỏ phần sau dấu cách (e.g. "SGN0005 (Hung Thanh...)" -> "SGN0005")
                    ma_clean = ma.split('(')[0].split(' ')[0].strip()
                    excluded.add(ma_clean.lower())
    except Exception as e:
        pass
    return excluded


# --- HÀM TỔNG HỢP LỢI NHUẬN LOẠI TRỪ CÁ NHÂN (TAB 6) ---
def get_profit_report_data_exclude_ca_nhan(file_source, time_input_str, df_source):
    excluded_set = get_ca_nhan_ma_trams(file_source)
    
    time_input_str = str(time_input_str).strip()
    target_months_years = []
    
    if '-' in time_input_str:
        p1, p2 = time_input_str.split('-')
        sm, sy = [int(x) for x in p1.strip().split('/')]
        em, ey = [int(x) for x in p2.strip().split('/')]
        curr_m, curr_y = sm, sy
        while (curr_y < ey) or (curr_y == ey and curr_m <= em):
            target_months_years.append((curr_m, curr_y))
            curr_m += 1
            if curr_m > 12:
                curr_m = 1
                curr_y += 1
    else:
        parts = time_input_str.split('/')
        if len(parts) == 2:
            try:
                target_months_years = [(int(parts[0]), int(parts[1]))]
            except:
                target_months_years = [(datetime.now().month, datetime.now().year)]
        else:
            try:
                target_year = int(time_input_str)
                target_months_years = [(m, target_year) for m in range(1, 13)]
            except:
                target_year = datetime.now().year
                target_months_years = [(m, target_year) for m in range(1, 13)]
    
    # 1. TỔNG TIỀN CHỦ NHÀ (loại trừ Cá nhân)
    chu_nha_totals = []
    for m, y in target_months_years:
        target_m_str = f"{m:02d}/{y}"
        df_pay = load_data_and_enrich_v3(file_source, target_m_str)
        if df_pay is not None and not df_pay.empty:
            df_pay_m = df_pay[df_pay["__is_due_this_month__"] == True].copy()
            # Loại trừ Cá nhân theo mã trạm
            df_pay_m = df_pay_m[
                ~df_pay_m["mã trạm"].astype(str).str.strip().str.lower().isin(excluded_set)
            ]
            tot = df_pay_m["__raw_amount__"].sum()
        else:
            tot = 0.0
        chu_nha_totals.append(tot)
    
    # 2. TỔNG DOANH THU CÁC NHÀ MẠNG (loại trừ Cá nhân)
    if hasattr(file_source, 'seek'):
        file_source.seek(0)
    xl = pd.ExcelFile(file_source)
    sheets = xl.sheet_names
    
    def sum_provider_exclude(provider_keyword):
        monthly_sums = [0.0] * len(target_months_years)
        target_sheet = next((s for s in sheets if provider_keyword.lower() in s.lower()), None)
        if not target_sheet: return monthly_sums
        
        if hasattr(file_source, 'seek'):
            file_source.seek(0)
        df = pd.read_excel(file_source, sheet_name=target_sheet)
        if df.empty: return monthly_sums
        
        # Lọc bỏ hàng Cá nhân (cột đầu)
        col0 = df.columns[0]
        df = df[df[col0].astype(str).str.strip().str.lower() != 'cá nhân'].copy()
        
        ma_col = df.columns[0]
        valid_cols = [c for c in df.columns if "ghi chú" not in str(c).lower() and "note" not in str(c).lower()]
        filtered = []
        for c in valid_cols:
            if str(c).lower().startswith("unnamed"):
                if not df[c].replace('', pd.NA).dropna().empty: filtered.append(c)
            else:
                filtered.append(c)
        last_col = filtered[-1] if filtered else df.columns[-1]
        
        m_col = None
        kw_list = ["trả/tháng", "thuê/tháng", "giá thuê", "đơn giá", "mức cước", "số tiền", "cước", "giá"]
        for kw in kw_list:
            for c in df.columns:
                c_str = str(c).lower()
                if kw in c_str and (c != last_col) and (c != ma_col):
                    m_col = c; break
            if m_col: break
        if not m_col: m_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        date_cols = [c for c in df.columns if c != ma_col and c != m_col and c != last_col]
        
        def parse_num(val):
            if pd.isna(val): return 0.0
            if isinstance(val, (int, float)): return float(val)
            s = str(val).strip()
            if s.endswith('.0'): s = s[:-2]
            digits = re.sub(r'\D', '', s)
            return float(digits) if digits else 0.0
        
        for _, row in df.iterrows():
            payment_val = parse_num(row[last_col])
            if payment_val == 0: continue
            
            dates = []
            for c in date_cols:
                val = row[c]
                if pd.notna(val):
                    if isinstance(val, (pd.Timestamp, datetime)): dates.append(pd.to_datetime(val))
                    else:
                        try: dates.append(pd.to_datetime(val))
                        except: pass
            
            for d in dates:
                for idx, (tm, ty) in enumerate(target_months_years):
                    if d.month == tm and d.year == ty:
                        monthly_sums[idx] += payment_val
        
        return monthly_sums
    
    viettel_totals = sum_provider_exclude("Viettel")
    vina_totals    = sum_provider_exclude("Vina")
    mobi_totals    = sum_provider_exclude("Mobi")
    
    # 3. BUILD DATAFRAME TỔNG
    records = []
    for i, (m, y) in enumerate(target_months_years):
        sum_rev = viettel_totals[i] + vina_totals[i] + mobi_totals[i]
        chu_nha = chu_nha_totals[i]
        profit  = (sum_rev / 1.1) - chu_nha
        records.append({
            "Tháng": f"{m:02d}/{y}",
            "Doanh thu Viettel": viettel_totals[i],
            "Doanh thu Vina":    vina_totals[i],
            "Doanh thu Mobi":    mobi_totals[i],
            "Tổng Doanh Thu":    sum_rev,
            "Tiền Chủ Nhà":      chu_nha,
            "Lợi nhuận":         profit
        })
    
    return pd.DataFrame(records), excluded_set

@st.cache_data(ttl=60) # Tự động xóa bộ nhớ đệm sau 60 giây để cập nhật dữ liệu mới từ GitHub
def load_data_and_enrich_v3(file_source, target_month_str):
    try:
        if hasattr(file_source, 'seek'):
            file_source.seek(0)
        xl = pd.ExcelFile(file_source)
        
        # 1. ĐỌC SHEET 1: Chi Tiết HD
        sheet_1_target = "Theo dõi HĐ_Chi tiết"
        target_sheet_1 = next((s for s in xl.sheet_names if sheet_1_target.lower() in s.lower()), None)
        if not target_sheet_1:
            st.error(f"⚠️ Không tìm thấy sheet có chứa chữ '{sheet_1_target}' trong file Excel.")
            return pd.DataFrame()

        df = pd.read_excel(file_source, sheet_name=target_sheet_1)
        available_cols = {normalize_str(col): col for col in df.columns}
        selected_actual = []
        for t_col in TARGET_COLUMNS:
            n_col = normalize_str(t_col)
            if n_col in available_cols:
                selected_actual.append(available_cols[n_col])
            else:
                partial = next((actual for norm, actual in available_cols.items() if n_col in norm or norm in n_col), None)
                selected_actual.append(partial)
                    
        valid_cols = [c for c in selected_actual if c is not None]
        df_filtered = df[valid_cols].copy()
        
        rename_dict = {actual: target for actual, target in zip(selected_actual, TARGET_COLUMNS) if actual is not None}
        df_filtered.rename(columns=rename_dict, inplace=True)
        
        for t_col in TARGET_COLUMNS:
            if t_col not in df_filtered.columns:
                df_filtered[t_col] = ''
                
        df_filtered = df_filtered[TARGET_COLUMNS]
        
        # LOẠI BỎ CÁC DÒNG RỖNG VÀ RÁC ĐẦU BẢNG ĐỂ ĐẾM CHÍNH XÁC SỐ TRẠM
        df_filtered = df_filtered[df_filtered["mã trạm"].notna()]
        df_filtered = df_filtered[df_filtered["mã trạm"].astype(str).str.strip() != ""]
        df_filtered = df_filtered[~df_filtered["mã trạm"].astype(str).str.isnumeric()]
        df_filtered = df_filtered[~df_filtered["mã trạm"].astype(str).str.lower().isin(["nan", "null", "mã trạm", "stt", "mã", "mã số", "filter", "tổng", "tổng cộng"])]
        df_filtered.reset_index(drop=True, inplace=True)
        
        # ĐỊNH DẠNG TẤT CẢ NGÀY THÁNG SANG mm/dd/yyyy
        for col in df_filtered.columns:
            if pd.api.types.is_datetime64_any_dtype(df_filtered[col]):
                df_filtered[col] = df_filtered[col].dt.strftime('%m/%d/%Y')
        df_filtered = df_filtered.fillna("")
        
        # 2. ĐỌC SHEET 2: LỊCH SỬ THANH TOÁN
        sheet_2_target = "Theo dõi thanh toán chủ nhà"
        target_sheet_2 = next((s for s in xl.sheet_names if sheet_2_target.lower() in s.lower()), None)
        
        df2 = pd.DataFrame()
        if target_sheet_2:
            df2 = pd.read_excel(file_source, sheet_name=target_sheet_2)
            
        # Tách tháng năm từ người dùng nhập
        now = datetime.now()
        t_month, t_year = now.month, now.year
        try:
            parts = target_month_str.split('/')
            if len(parts) == 2:
                t_month, t_year = int(parts[0]), int(parts[1])
        except:
            pass
            
        if not df2.empty:
            df_final = enrich_payment_data(df_filtered, df2, t_month, t_year)
        else:
            for c in EXTRA_PAY_COLS: df_filtered[c] = "-"
            df_filtered["__raw_amount__"] = 0.0
            df_filtered["__is_due_this_month__"] = False
            df_final = df_filtered
            
        return df_final
    except Exception as e:
        st.error(f"⚠️ Có lỗi trong quá trình đọc Excel: {e}")
        return pd.DataFrame()

# --- SIDEBAR VÀ NHÚNG DATA ---
st.sidebar.header("📁 Dữ Liệu Báo Cáo")

current_mm_yyyy = datetime.now().strftime('%m/%Y')

# Hỗ trợ tự động nhận diện cả chữ hoa chữ thường
DEFAULT_FILE = ""
if os.path.exists("data.xlsx"):
    DEFAULT_FILE = "data.xlsx"
elif os.path.exists("Data.xlsx"):
    DEFAULT_FILE = "Data.xlsx"

df_source = pd.DataFrame()

if DEFAULT_FILE:
    st.sidebar.success(f"✅ Đã kết nối tự động với CSDL gốc: `{DEFAULT_FILE}`")
    df_source = load_data_and_enrich_v3(DEFAULT_FILE, current_mm_yyyy)
else:
    st.sidebar.warning(f"⚠️ Vùng Nhúng Ngầm Trống! Bạn hãy File Excel vào GitHub nhé.")
    uploaded_file = st.sidebar.file_uploader("Hoặc tải file Excel tạm thời lên đây:", type=["xlsx", "xls"])
    if uploaded_file is not None:
        df_source = load_data_and_enrich_v3(uploaded_file, current_mm_yyyy)

# Khu vực hiển thị kết quả Thẻ Bài
def render_cards(df_to_render, is_payment_tab=False):
    if len(df_to_render) > 50:
        st.warning(f"⚠️ Ứng dụng hiển thị mượt dạng thẻ dọc cho 50 trạm đầu tiên để chống đứng máy. Anh/chị xem toàn bộ danh sách ở Bảng Tổng Hợp bên dưới.")
        display_cards = df_to_render.head(50)
    else:
        display_cards = df_to_render
        
    for index, row in display_cards.iterrows():
        tram_id = str(row["mã trạm"]) if pd.notna(row["mã trạm"]) and str(row["mã trạm"]) != "" else "Không Mẫu"
        # Thẻ màu khác nhau nếu là có thanh toán
        title = f"💰 Thanh toán Trạm: {tram_id}" if is_payment_tab else f"📌 Thông tin Trạm: {tram_id}"
        
        with st.expander(title, expanded=True):
            for col in DISPLAY_COLUMNS:
                val = row[col]
                # High-light các cột thanh toán
                if col in EXTRA_PAY_COLS:
                    st.markdown(f"<span style='color:#a8d1ff;'>**{col}:** &nbsp;&nbsp; {val}</span>", unsafe_allow_html=True)
                else:
                    if pd.isna(val) or str(val).strip() == "": val = "-"
                    st.markdown(f"**{col}:** &nbsp;&nbsp; {val}") 
                    
# --- GIAO DIỆN HIỂN THỊ CHÍNH ---
if not df_source.empty:
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
        "🔍 LỌC THÔNG TIN MÃ TRẠM",
        "💵 DS TRẠM TT CHỦ NHÀ",
        "💰 DOANH THU CÁC NHÀ MẠNG",
        "📈 BÁO CÁO LỢI NHUẬN CÔNG TY",
        "💸 CÚ PHÁP CHUYỂN KHOẢN APP NH",
        "🏛️ BÁO CÁO LỢI NHUẬN - LOẠI TRỪ CÁ NHÂN",
        "🗺️ BẢN ĐỒ VỊ TRÍ CÁC TRẠM",
        "⏱️ TRA CỨU THỜI GIAN HOÀN VỐN",
        "🧮 TÍNH THỜI GIAN HOÀN VỐN"
    ])

    # ------------ TAB 1: TRA CỨU TRẠM BẤT KỲ ------------
    with tab1:
        with st.form(key='search_form'):
            st.markdown("### 🔍 Phễu Tra Cứu (Có bổ sung Data Sheet 2)")
            input_text = st.text_area("Dán mã trạm cần tìm (ngăn cách bởi dấu phẩy hoặc enter xuống dòng):", height=100)
            submit_search = st.form_submit_button(label="🔍 TÌM KIẾM CHI TIẾT", use_container_width=True)
        
        if submit_search:
            df_display = df_source.copy()
            if input_text.strip():
                target_stations = [s.strip().lower() for s in input_text.replace(',', '\n').split('\n') if s.strip()]
                if target_stations:
                    mask = df_display["mã trạm"].astype(str).str.strip().str.lower().isin(target_stations)
                    df_display = df_display[mask]
                    
            if df_display.empty:
                st.warning("❌ Rất tiếc! Không tìm thấy mã trạm nào khớp với dữ liệu bạn cung cấp.")
            else:
                st.success(f"✅ Móc nối thành công! Bắt được **{len(df_display)}** trạm.")
                
                st.markdown('<h3 style="color:red; font-weight:bold;">📊 Tổng Hợp Lưới Ngang (Xem trọn bộ Hàng Ngang)</h3>', unsafe_allow_html=True)
                df_clean_tab1 = df_display.drop(["__raw_amount__", "__is_due_this_month__"], axis=1, errors='ignore')
                # Chèn thêm cột Số thứ tự ở vị trí đầu tiên
                df_clean_tab1.insert(0, 'STT', range(1, len(df_clean_tab1) + 1))
                
                # HTML Đỏ Đậm Style cho Tab 1
                st.markdown("""
                <style>
                .red-header-table-general { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                .red-header-table-general th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 15px; }
                .red-header-table-general td { border: 1px solid #e0e0e0; padding: 8px; font-size: 14px; }
                .red-header-table-general tr:nth-child(even) { background-color: #f9f9f9; }
                .red-header-table-general tr:hover { background-color: #f1f1f1; }
                </style>
                """, unsafe_allow_html=True)
                
                html_report_1 = df_clean_tab1.to_html(index=False, classes="red-header-table-general", escape=False)
                st.markdown(html_report_1, unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("### 🏷️ Chi Tiết Dạng Thẻ (Dành cho Vuốt Trên Điện Thoại)")
                render_cards(df_display, is_payment_tab=False)
                
    # ------------ TAB 2: QUẢN LÝ TỔNG THANH TOÁN THÁNG ------------
    with tab2:
        st.markdown(f"### 💵 Quản Lý Phân Luồng Thanh Toán Chủ Nhà")
        with st.form(key='payment_form'):
            st.info("Nhập tùy chọn tháng năm để truy vấn dòng tiền tương ứng. Nhấn nút TRA CỨU bên dưới để thực thi.")
            # Chèn ô TEXT BOX chọn tháng trực tiếp vào Tab 2
            month_input_tab2 = st.text_input("📅 Nhập định dạng Tháng/Năm Tra Cứu (MM/YYYY):", value=current_mm_yyyy)
            
            search_tab2 = st.text_input("🔍 Tra cứu cụ thể một (hoặc nhiều) mã trạm trong DS của tháng (Để trống là Tính Tổng Tất Cả):", placeholder="Ví dụ: HCM001, HCM002...")
            
            c1, c2 = st.columns(2)
            with c1:
                date_start_tab2 = st.text_input("⏳ Từ ngày (Để trống lấy từ đầu tháng):", placeholder="MM/DD/YYYY. Ví dụ: 03/01/2026")
            with c2:
                date_end_tab2 = st.text_input("⏳ Đến ngày (Để trống lấy đến cuối tháng):", placeholder="MM/DD/YYYY. Ví dụ: 03/25/2026")
            
            submit_payment_filter = st.form_submit_button(label="🔍 TRA CỨU DANH SÁCH", use_container_width=True)
            
        if submit_payment_filter:
            if not validate_month_year(month_input_tab2):
                display_error("Bạn đã nhập sai định dạng tháng/năm, vui lòng nhập đúng để hệ thống hiển thị kết quả, xin cám ơn!")
            else:
                # Nhồi lại Data Engine đặc biệt theo Option Tháng vừa nhập!
                f_source = DEFAULT_FILE if DEFAULT_FILE else uploaded_file
                df_pay_source = load_data_and_enrich_v3(f_source, month_input_tab2)
                df_pay_display = df_pay_source[df_pay_source["__is_due_this_month__"] == True].copy()
                
                # Lọc theo trạm cụ thể nếu người dùng có gõ
                if search_tab2.strip():
                    target_stations_2 = [s.strip().lower() for s in search_tab2.replace(',', '\n').split('\n') if s.strip()]
                    if target_stations_2:
                        mask2 = df_pay_display["mã trạm"].astype(str).str.strip().str.lower().isin(target_stations_2)
                        df_pay_display = df_pay_display[mask2]
                        
                # Lọc đếm ngược Tới Mốc Khoảng Ngày (Phục vụ Chốt Quỹ Giải Ngân Kế Toán)
                has_date_error_2 = False
                v1_ok, e1 = validate_input_date(date_start_tab2)
                v2_ok, e2 = validate_input_date(date_end_tab2)
                
                if not v1_ok:
                    has_date_error_2 = True
                    display_error(e1)
                elif not v2_ok:
                    has_date_error_2 = True
                    display_error(e2)
                elif date_start_tab2.strip() or date_end_tab2.strip():
                    try:
                        temp_dt = pd.to_datetime(df_pay_display['Ngày tới hạn TT trong tháng'], format='%m/%d/%Y', errors='coerce')
                        mask_date = pd.Series([True] * len(df_pay_display), index=df_pay_display.index)
                        
                        if date_start_tab2.strip():
                            start_dt = pd.to_datetime(date_start_tab2.strip(), format='%m/%d/%Y')
                            mask_date &= (temp_dt >= start_dt)
                            
                        if date_end_tab2.strip():
                            end_dt = pd.to_datetime(date_end_tab2.strip(), format='%m/%d/%Y')
                            mask_date &= (temp_dt <= end_dt)
                            
                        mask_date &= temp_dt.notna()
                        df_pay_display = df_pay_display[mask_date]
                    except Exception:
                        has_date_error_2 = True
                        display_error("Lỗi định dạng hệ thống khi xử lý ngày tháng!")
            
            if has_date_error_2:
                pass
            elif df_pay_display.empty:
                st.warning(f"❌ Rất tiếc, Không tìm thấy Trạm nào cần giải ngân thỏa mãn các lớp điều kiện trong tháng {month_input_tab2}.")
            else:
                # SẮP XẾP TỪ NGÀY ĐẦU THÁNG ĐẾN CUỐI THÁNG CHUẨN XÁC
                df_pay_display = df_pay_display.sort_values(
                    by="Ngày tới hạn TT trong tháng", 
                    key=lambda col: pd.to_datetime(col, format='%m/%d/%Y', errors='coerce')
                )
                
                total_stations = len(df_pay_display)
                total_amount = df_pay_display["__raw_amount__"].sum()
                
                st.snow()
                if date_start_tab2.strip() or date_end_tab2.strip():
                    msg_start = date_start_tab2.strip() if date_start_tab2.strip() else "Đầu tháng"
                    msg_end = date_end_tab2.strip() if date_end_tab2.strip() else "Cuối tháng"
                    st.success(f"🔥 **BÁO CÁO GIẢI NGÂN GẤP (CHỈ TÍNH CÁC HĐ TỪ {msg_start} ĐẾN {msg_end} CỦA THÁNG {month_input_tab2}):**")
                else:
                    st.success(f"🔥 **TỔNG KẾT BÁO CÁO NHANH LŨY KẾ CẢ THÁNG NAY ({month_input_tab2}):**")
                    
                colA, colB = st.columns(2)
                colA.metric("🏢 Tổng số trạm hiển thị:", f"{total_stations} trạm")
                colB.metric("💰 Tổng tiền giải ngân:", f"{total_amount:,.0f} VNĐ")
                
                st.markdown("---")
                st.markdown('<h3 style="color:red; font-weight:bold;">📊 Tổng Hợp Lưới Ngang (Báo cáo Lọc Dạng Bảng)</h3>', unsafe_allow_html=True)
                df_clean_tab2 = df_pay_display.drop(["__raw_amount__", "__is_due_this_month__"], axis=1, errors='ignore')
                # Chèn thêm cột Số thứ tự ở vị trí đầu tiên
                df_clean_tab2.insert(0, 'STT', range(1, len(df_clean_tab2) + 1))
                
                # HTML Đỏ Đậm Style cho Tab 2
                st.markdown("""
                <style>
                .red-header-table-general { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                .red-header-table-general th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 15px; }
                .red-header-table-general td { border: 1px solid #e0e0e0; padding: 8px; font-size: 14px; }
                .red-header-table-general tr:nth-child(even) { background-color: #f9f9f9; }
                .red-header-table-general tr:hover { background-color: #f1f1f1; }
                </style>
                """, unsafe_allow_html=True)
                
                html_report_2 = df_clean_tab2.to_html(index=False, classes="red-header-table-general", escape=False)
                st.markdown(html_report_2, unsafe_allow_html=True)

                st.markdown("---")
                st.markdown("### 🏷️ Chi Tiết Các Trạm (Dạng Thẻ Điện Thoại Phóng To)")
                render_cards(df_pay_display, is_payment_tab=True)
                
                # Nút tải xuống cho báo cáo Tab 2
                output2 = io.BytesIO()
                with pd.ExcelWriter(output2, engine='openpyxl') as writer:
                    df_clean_tab2.to_excel(writer, index=False, sheet_name='TraCuuThang')
                excel_data2 = output2.getvalue()
                st.download_button(
                    label="🔽 NHẤN TẢI XUỐNG BÁO CÁO (EXCEL)",
                    data=excel_data2,
                    file_name=f"Bao_Cao_Thanh_Toan_{month_input_tab2.replace('/','_')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

    # ------------ TAB 3: DOANH THU NHÀ MẠNG ------------
    with tab3:
        st.markdown(f"### 💰 Báo Cáo Nhận Doanh Thu Từ Các Nhà Mạng")
        with st.form(key='revenue_form'):
            st.info("Hệ thống tự động tra cứu Dữ liệu Doanh thu từ 3 Sheet (Trạm Viettel thanh toán, Trạm Vina thanh toán, Trạm Mobi thanh toán). Cột SỐ TIỀN THANH TOÁN (1 kỳ) sẽ móc mặc định từ Cột Cuối Cùng của mỗi bảng trên file Excel!")
            month_input_tab3 = st.text_input("📅 Nhập định dạng Tháng/Năm Doanh Thu (MM/YYYY):", value=current_mm_yyyy)
            submit_revenue = st.form_submit_button(label="🔍 LÊN BÁO CÁO DOANH THU", use_container_width=True)
            
        if submit_revenue:
            if not validate_month_year(month_input_tab3):
                display_error("Bạn đã nhập sai định dạng tháng/năm, vui lòng nhập đúng để hệ thống hiển thị kết quả, xin cám ơn!")
            else:
                f_source = DEFAULT_FILE if DEFAULT_FILE else uploaded_file
                if f_source is None:
                    st.warning("⚠️ Không tìm thấy File dữ liệu (Upload hoặc Local) để phân tích Doanh thu!")
                else:
                    df_viettel, df_vina, df_mobi = load_revenue_data_v2(f_source, month_input_tab3)
                    
                    sv = df_viettel['__raw_payment__'].sum() if (df_viettel is not None and not df_viettel.empty) else 0.0
                    svina = df_vina['__raw_payment__'].sum() if (df_vina is not None and not df_vina.empty) else 0.0
                    smobi = df_mobi['__raw_payment__'].sum() if (df_mobi is not None and not df_mobi.empty) else 0.0
                    total_all = sv + svina + smobi
                    
                    st.snow()
                    st.success(f"🔥 **BÁO CÁO DOANH THU CÁC NHÀ MẠNG THÁNG {month_input_tab3} HOÀN TẤT!**")
                    
                    # Inject Custom HTML Table Style 100% Force Red Bold
                    st.markdown("""
                    <style>
                    .red-header-table { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                    .red-header-table th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 15px; }
                    .red-header-table td { border: 1px solid #e0e0e0; padding: 8px; font-size: 14px; }
                    .red-header-table tr:nth-child(even) { background-color: #f9f9f9; }
                    .red-header-table tr:hover { background-color: #f1f1f1; }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    st.markdown('<h3 style="color:red; font-weight:bold;">🌐 Bảng 1: Bảng Đầu Tiên - Tổng Kết Doanh Thu Trong Tháng</h3>', unsafe_allow_html=True)
                    df_summ = pd.DataFrame({
                        "Tháng Công ty có doanh thu": [month_input_tab3],
                        "sum số tiền Viettel thanh toán": [f"{sv:,.0f}"],
                        "sum số tiền Vina thanh toán": [f"{svina:,.0f}"],
                        "sum số tiền Mobi thanh toán": [f"{smobi:,.0f}"],
                        "sum tổng số tiền Viettel+Vina+Mobi thanh toán": [f"{total_all:,.0f}"]
                    })
                    # Chèn STT
                    df_summ.insert(0, 'STT', range(1, len(df_summ) + 1))
                    
                    # Render Html Table
                    html_summ = df_summ.to_html(index=False, classes="red-header-table", escape=False)
                    st.markdown(html_summ, unsafe_allow_html=True)
                    
                    def render_provider_table(df_prov, name, b_num):
                        if df_prov is not None and not df_prov.empty:
                            st.markdown(f"---")
                            st.markdown(f'<h3 style="color:red; font-weight:bold;">📡 Bảng {b_num}: Doanh thu Trạm {name} TT</h3>', unsafe_allow_html=True)
                            df_d = df_prov.drop(['__raw_payment__'], axis=1, errors='ignore')
                            df_d.insert(0, 'STT', range(1, len(df_d) + 1))
                            # Render Html Table
                            html_d = df_d.to_html(index=False, classes="red-header-table", escape=False)
                            st.markdown(html_d, unsafe_allow_html=True)
                        else:
                            st.markdown(f"---")
                            st.markdown(f'<h3 style="color:red; font-weight:bold;">📡 Bảng {b_num}: Doanh thu Trạm {name} TT</h3>', unsafe_allow_html=True)
                            st.info(f"Không có số liệu hoặc thiếu Sheet '{name}' chưa đúng tên theo yêu cầu.")
                            
                    render_provider_table(df_viettel, "Viettel", 2)
                    render_provider_table(df_vina, "Vina", 3)
                    render_provider_table(df_mobi, "Mobi", 4)
                    
                    st.markdown("---")
                    out_rev = io.BytesIO()
                    with pd.ExcelWriter(out_rev, engine='openpyxl') as writer:
                        df_summ.to_excel(writer, index=False, sheet_name='Tong_Hop_Doanh_Thu')
                        if df_viettel is not None and not df_viettel.empty:
                            df_viettel.drop(['__raw_payment__'], axis=1, errors='ignore').to_excel(writer, index=False, sheet_name='Viettel')
                        if df_vina is not None and not df_vina.empty:
                            df_vina.drop(['__raw_payment__'], axis=1, errors='ignore').to_excel(writer, index=False, sheet_name='Vina')
                        if df_mobi is not None and not df_mobi.empty:
                            df_mobi.drop(['__raw_payment__'], axis=1, errors='ignore').to_excel(writer, index=False, sheet_name='Mobi')
                            
                    st.download_button(
                        label="🔽 TẢI XUỐNG FILE TỔNG HỢP DOANH THU (EXCEL)",
                        data=out_rev.getvalue(),
                        file_name=f"Bao_Cao_Doanh_Thu_Nha_Mang_{month_input_tab3.replace('/','_')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )

    # ------------ TAB 4: BÁO CÁO LỢI NHUẬN CÔNG TY ------------
    with tab4:
        st.markdown(f"### 📈 Báo Cáo KQKD Công Ty (Doanh Thu vs Chi Phí)")
        with st.form(key='profit_yearly_form'):
            st.info("💡 Điền vào chữ Tùy chọn 1 HOẶC Tùy chọn 2 bên dưới rồi nhấn nút.")
            time_input_range = st.text_input("📅 Tùy chọn 1: Khoảng tháng (Ví dụ: 05/2026 - 07/2026):", placeholder="MM/YYYY - MM/YYYY")
            time_input_tab4 = st.text_input("📅 Tùy chọn 2: Một Năm (YYYY) hoặc Một Tháng cụ thể (MM/YYYY):", placeholder="Ví dụ: 03/2026", value=str(datetime.now().year))
            submit_profit = st.form_submit_button(label="🔍 TỔNG HỢP LỢI NHUẬN TÀI CHÍNH", use_container_width=True)
            
        if submit_profit:
            actual_time_str = time_input_range.strip() if time_input_range.strip() else time_input_tab4.strip()
            is_valid_t4 = validate_month_year_or_year(actual_time_str)
            f_source = DEFAULT_FILE if DEFAULT_FILE else uploaded_file
            if not is_valid_t4:
                display_error("Bạn đã nhập sai định dạng tháng/năm, vui lòng nhập đúng để hệ thống hiển thị kết quả, xin cám ơn!")
            elif f_source is None:
                st.warning("⚠️ Không tìm thấy File dữ liệu (Upload hoặc Local) để phân tích Doanh thu!")
            else:
                with st.spinner(f"Hệ thống đang xào nấu luồng Doanh thu & Chi phí cho mốc thời gian {actual_time_str}... (Vui lòng chờ vài giây)"):
                    df_report_raw = get_profit_report_data(f_source, actual_time_str, df_source)
                
                st.snow()
                st.success(f"🔥 CẬP NHẬT HOÀN TẤT LỢI NHUẬN TÀI CHÍNH CHO KỲ: {actual_time_str}!")
                
                # Biểu đồ Cột Song Song thay vì Chồng lên nhau (Dùng Altair có sẵn của Streamlit)
                import altair as alt
                st.markdown(f'<h3 style="color:red; font-weight:bold;">📊 Biểu đồ Lợi Nhuận Kỳ {actual_time_str}</h3>', unsafe_allow_html=True)
                chart_data = df_report_raw.rename(columns={
                    "Tổng Doanh Thu": "Tổng Doanh Thu",
                    "Tiền Chủ Nhà": "Tổng Tiền Trả Chủ Nhà",
                    "Lợi nhuận": "Lợi Nhuận Công Ty"
                }).set_index("Tháng")[["Tổng Doanh Thu", "Tổng Tiền Trả Chủ Nhà", "Lợi Nhuận Công Ty"]]
                
                df_melted = chart_data.reset_index().melt(
                    id_vars=['Tháng'], 
                    value_vars=['Tổng Doanh Thu', 'Tổng Tiền Trả Chủ Nhà', 'Lợi Nhuận Công Ty'], 
                    var_name='Chỉ Tiêu', 
                    value_name='Số Tiền (VNĐ)'
                )
                
                # Biểu đồ 3 cột tách biệt (chuẩn song song, thu nhỏ bề ngang)
                chart = alt.Chart(df_melted).mark_bar(size=18).encode(
                    x=alt.X('Chỉ Tiêu:N', axis=alt.Axis(title=None, labels=False, ticks=False), sort=['Tổng Doanh Thu', 'Tổng Tiền Trả Chủ Nhà', 'Lợi Nhuận Công Ty']),
                    y=alt.Y('Số Tiền (VNĐ):Q', title='Giá Trị (VNĐ)'),
                    color=alt.Color('Chỉ Tiêu:N', scale=alt.Scale(
                        domain=['Tổng Doanh Thu', 'Tổng Tiền Trả Chủ Nhà', 'Lợi Nhuận Công Ty'],
                        range=['#FF0000', '#800080', '#00008B']
                    ), legend=alt.Legend(orient='top', title=None)),
                    column=alt.Column('Tháng:N', header=alt.Header(title=None, labelOrient='bottom', labelAlign='center'))
                ).properties(
                    width=75 # Ép chiều rộng mỗi tháng là 75 pixel để cột luôn nhỏ xinh
                ).configure_view(
                    stroke='transparent'
                )
                
                # Tắt tự động phóng to của Streamlit để biểu đồ giữ đúng form nhỏ
                st.altair_chart(chart, use_container_width=False)
                
                # Bảng chi tiết
                st.markdown(f'<h3 style="color:red; font-weight:bold;">📑 Bảng Tổng Hợp Dòng Tiền Kỳ {actual_time_str}</h3>', unsafe_allow_html=True)
                df_report_display = df_report_raw.copy()
                
                # SUM TỔNG CỘNG
                sum_row_data = {
                    "Tháng": "TỔNG CỘNG",
                    "Doanh thu Viettel": getattr(df_report_display["Doanh thu Viettel"], "sum")() if "Doanh thu Viettel" in df_report_display.columns else 0,
                    "Doanh thu Vina": getattr(df_report_display["Doanh thu Vina"], "sum")() if "Doanh thu Vina" in df_report_display.columns else 0,
                    "Doanh thu Mobi": getattr(df_report_display["Doanh thu Mobi"], "sum")() if "Doanh thu Mobi" in df_report_display.columns else 0,
                    "Tổng Doanh Thu": getattr(df_report_display["Tổng Doanh Thu"], "sum")() if "Tổng Doanh Thu" in df_report_display.columns else 0,
                    "Tiền Chủ Nhà": getattr(df_report_display["Tiền Chủ Nhà"], "sum")() if "Tiền Chủ Nhà" in df_report_display.columns else 0,
                    "Lợi nhuận": getattr(df_report_display["Lợi nhuận"], "sum")() if "Lợi nhuận" in df_report_display.columns else 0
                }
                sum_df = pd.DataFrame([sum_row_data])
                df_report_display = pd.concat([sum_df, df_report_display], ignore_index=True)
                
                for col in ["Doanh thu Viettel", "Doanh thu Vina", "Doanh thu Mobi", "Tổng Doanh Thu", "Tiền Chủ Nhà", "Lợi nhuận"]:
                    df_report_display[col] = df_report_display[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x != 0 else "-")
                    
                n_data = len(df_report_display) - 1  # exclude tổng cộng row
                df_report_display.insert(0, 'STT', ["-"] + list(range(1, n_data + 1)))
                
                df_report_display.rename(columns={
                    "Tháng": "Tháng mm/yyyy",
                    "Doanh thu Viettel": "Doanh thu Viettel theo tháng",
                    "Doanh thu Vina": "Doanh thu Vina theo tháng",
                    "Doanh thu Mobi": "Doanh thu Mobi theo tháng",
                    "Tổng Doanh Thu": "Sum doanh thu Viettel+Vina+Mobi theo tháng",
                    "Tiền Chủ Nhà": "Tổng tiền phải trả chủ nhà theo tháng",
                    "Lợi nhuận": "Lợi nhuận sau thuế của Công ty"
                }, inplace=True)
                
                # Inject Custom HTML Table Style (Màu Đỏ Đậm cho Tab 4 Lợi Nhuận)
                st.markdown("""
                <style>
                .red-header-tab4 { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                .red-header-tab4 th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 15px; }
                .red-header-tab4 td { border: 1px solid #e0e0e0; padding: 8px; font-size: 14px; }
                .red-header-tab4 tr:nth-child(even) { background-color: #f9f9f9; }
                .red-header-tab4 tr:hover { background-color: #f1f1f1; }
                .red-header-tab4 tbody tr:first-child td { color: #28a745 !important; font-weight: 900 !important; font-size: 1.5em !important; background-color: #e8f5e9 !important; }
                </style>
                """, unsafe_allow_html=True)
                
                html_report = df_report_display.to_html(index=False, classes="red-header-tab4", escape=False)
                st.markdown(html_report, unsafe_allow_html=True)
                
                out_prf = io.BytesIO()
                with pd.ExcelWriter(out_prf, engine='openpyxl') as writer:
                    # Chống lỗi sập do dấu gạch chéo không được dùng làm tên Sheet Excel
                    safe_sheet_name = str(actual_time_str).replace('/', '_').replace(' ', '')
                    df_report_display.to_excel(writer, index=False, sheet_name=f'Loi_Nhuan_{safe_sheet_name}')
                    
                safe_file_name = f"Bao_Cao_Loi_Nhuan_{safe_sheet_name}.xlsx"
                st.download_button(
                    label=f"🔽 TẢI XUỐNG BÁO CÁO LỢI NHUẬN KỲ {actual_time_str} (EXCEL)",
                    data=out_prf.getvalue(),
                    file_name=safe_file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

    # ------------ TAB 5: CÚ PHÁP CHUYỂN KHOẢN NH ------------
    with tab5:
        st.markdown(f"### 💸 Tự Động Hóa Cú Pháp Chuyển Khoản Ngân Hàng")
        with st.form(key='subject_form'):
            st.info("💡 Tra cứu theo định dạng Tháng/Năm (MM/YYYY) để kết xuất Cú pháp Content cho Ngân hàng.")
            month_input_tab5 = st.text_input("📅 Nhập định dạng Tháng/Năm Tra Cứu (MM/YYYY):", value=datetime.now().strftime('%m/%Y'))
            
            c1, c2 = st.columns(2)
            with c1:
                date_start_tab5 = st.text_input("⏳ Từ ngày (Để trống lấy từ đầu tháng):", placeholder="MM/DD/YYYY. Ví dụ: 03/01/2026")
            with c2:
                date_end_tab5 = st.text_input("⏳ Đến ngày (Để trống lấy đến cuối tháng):", placeholder="MM/DD/YYYY. Ví dụ: 03/25/2026")
                
            submit_subject = st.form_submit_button(label="🔍 TẠO DANH SÁCH COPY (NGÂN HÀNG)", use_container_width=True)
            
        if submit_subject:
            if not validate_month_year(month_input_tab5):
                display_error("Bạn đã nhập sai định dạng tháng/năm, vui lòng nhập đúng để hệ thống hiển thị kết quả, xin cám ơn!")
            else:
                f_source = DEFAULT_FILE if DEFAULT_FILE else uploaded_file
                if f_source is None:
                    st.warning("⚠️ Không tìm thấy File dữ liệu (Upload hoặc Local) để phân tích!")
                else:
                    with st.spinner(f"Hệ thống đang trích xuất Nội dung Chuyển khoản trong {month_input_tab5}..."):
                        df_pay_source_5 = load_data_and_enrich_v3(f_source, month_input_tab5)
                        df_pay_display_5 = df_pay_source_5[df_pay_source_5["__is_due_this_month__"] == True].copy()
                    
                    # Bộ lọc Khoảng thời gian
                    has_date_error_5 = False
                    v1_ok, e1 = validate_input_date(date_start_tab5)
                    v2_ok, e2 = validate_input_date(date_end_tab5)
                    
                    if not v1_ok:
                        has_date_error_5 = True
                        display_error(e1)
                    elif not v2_ok:
                        has_date_error_5 = True
                        display_error(e2)
                    elif date_start_tab5.strip() or date_end_tab5.strip():
                        try:
                            temp_dt = pd.to_datetime(df_pay_display_5['Ngày tới hạn TT trong tháng'], format='%m/%d/%Y', errors='coerce')
                            mask_date = pd.Series([True] * len(df_pay_display_5), index=df_pay_display_5.index)
                            
                            if date_start_tab5.strip():
                                start_dt = pd.to_datetime(date_start_tab5.strip(), format='%m/%d/%Y')
                                mask_date &= (temp_dt >= start_dt)
                                
                            if date_end_tab5.strip():
                                end_dt = pd.to_datetime(date_end_tab5.strip(), format='%m/%d/%Y')
                                mask_date &= (temp_dt <= end_dt)
                                
                            mask_date &= temp_dt.notna()
                            df_pay_display_5 = df_pay_display_5[mask_date]
                        except Exception:
                            has_date_error_5 = True
                            display_error("Lỗi định dạng hệ thống khi xử lý ngày tháng!")
                
                if has_date_error_5:
                    pass
                elif df_pay_display_5.empty:
                    st.warning(f"❌ Không tìm thấy Hợp đồng Trạm nào cần Chuyển Khoản thỏa mãn điều kiện lọc trong tháng {month_input_tab5}.")
                else:
                    # Sắp xếp lịch giải ngân từ sớm đến trễ
                    df_pay_display_5 = df_pay_display_5.sort_values(
                        by="Ngày tới hạn TT trong tháng", 
                        key=lambda col: pd.to_datetime(col, format='%m/%d/%Y', errors='coerce')
                    )
                    
                    total_stations_5 = len(df_pay_display_5)
                    total_amount_5 = df_pay_display_5["__raw_amount__"].sum()
                    
                    st.snow()
                    if date_start_tab5.strip() or date_end_tab5.strip():
                        msg_start = date_start_tab5.strip() if date_start_tab5.strip() else "Đầu tháng"
                        msg_end = date_end_tab5.strip() if date_end_tab5.strip() else "Cuối tháng"
                        st.success(f"🔥 Khởi tạo Cú Pháp thành công cho **{total_stations_5}** hợp đồng (Từ {msg_start} đến {msg_end})!")
                    else:
                        st.success(f"🔥 Đã khởi tạo Cú Pháp Chuyển Khoản thành công cho toàn bộ **{total_stations_5}** hợp đồng Chủ Nhà của Cả tháng!")
                        
                    colA, colB = st.columns(2)
                    colA.metric("🏢 Tổng số trạm hiển thị:", f"{total_stations_5} trạm")
                    colB.metric("💰 Tổng tiền giải ngân:", f"{total_amount_5:,.0f} VNĐ")
                    
                    def generate_subject(row):
                        ma_tram = str(row.get("mã trạm", "")).strip().upper()
                        if pd.isna(ma_tram) or ma_tram == "NAN": ma_tram = ""
                        
                        # Xóa bỏ các Cụm tên riêng dính liền dễ phát sinh
                        ma_tram = ma_tram.replace('_A DŨNG', '').replace('_A DUNG', '').replace('A DŨNG', '').replace('A DUNG', '')
                        
                        # Bắt đầu "MÁY SẤY" loại bỏ các ký tự đặc biệt: -, ->, -->, khoảng trắng, phẩy, chấm, gạch dưới
                        for char in ['>', '-', ' ', ',', '.', '_']:
                            ma_tram = ma_tram.replace(char, '')
                        
                        raw_hd = str(row.get("Số HĐ với chủ nhà", "")).strip().upper()
                        if pd.isna(raw_hd) or raw_hd == "NAN": raw_hd = ""
                        
                        # Trích lọc HĐ từ đầu đến hết DKV
                        h_idx = raw_hd.find('DKV')
                        if h_idx != -1:
                            hd_clean = raw_hd[:h_idx+3]
                        else:
                            hd_clean = raw_hd
                            
                        # Loại bỏ "-", "/" và dấu cách
                        hd_clean = hd_clean.replace('-', '').replace('/', '').replace(' ', '')
                        
                        date_str = ""
                        try:
                            d_curr_str = str(row.get("Ngày tới hạn TT trong tháng", "")).strip()
                            d_next_str = str(row.get("Ngày đến hạn TT kỳ tiếp theo", "")).strip()
                            
                            d_curr = datetime.strptime(d_curr_str, '%m/%d/%Y')
                            d_next = datetime.strptime(d_next_str, '%m/%d/%Y')
                            
                            # Lùi lại 1 ngày so với kỳ tiếp theo
                            d_end = d_next - timedelta(days=1)
                            
                            str_start = d_curr.strftime('%d%m%Y')
                            str_end = d_end.strftime('%d%m%Y')
                            date_str = f"tu ngay {str_start} den {str_end}"
                        except Exception:
                            date_str = "tu ngay ... den ..."
                            
                        # Format chuỗi tiêu chuẩn
                        subject = f"Thanh toan thue vi tri {ma_tram} theo HD {hd_clean} {date_str}"
                        return subject

                    df_pay_display_5["Cú pháp nội dung (Copy App)"] = df_pay_display_5.apply(generate_subject, axis=1)
                    
                    # Các cột cố định cần xuất
                    cols_to_show = [
                        "mã trạm", 
                        "giá thuê chủ nhà", 
                        "Số tiền cần thanh toán", 
                        "Số TK chủ nhà", 
                        "Chủ tài khoản", 
                        "Tên Ngân Hàng", 
                        "Ngày tới hạn TT trong tháng", 
                        "Ngày đến hạn TT kỳ tiếp theo", 
                        "Cú pháp nội dung (Copy App)"
                    ]
                    
                    # Lọc lấy cột thực tế có trong mảng
                    existing_cols = []
                    for c in cols_to_show:
                        if c in df_pay_display_5.columns:
                            existing_cols.append(c)
                        else:
                            # Khớp linh động hoa thường
                            match = [orig for orig in df_pay_display_5.columns if str(orig).strip().lower() == str(c).lower()]
                            if match: existing_cols.append(match[0])
                            
                    df_clean_tab5 = df_pay_display_5[existing_cols].copy()
                    df_clean_tab5.insert(0, 'STT', range(1, len(df_clean_tab5) + 1))
                    
                    st.markdown('<h3 style="color:red; font-weight:bold;">🏷️ Lưới Chi Tiết Cú Pháp Giao Dịch Ngân Hàng</h3>', unsafe_allow_html=True)
                    st.markdown("""
                    <style>
                    .red-header-table-general { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                    .red-header-table-general th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 15px; }
                    .red-header-table-general td { border: 1px solid #e0e0e0; padding: 8px; font-size: 14px; }
                    .red-header-table-general tr:nth-child(even) { background-color: #f9f9f9; }
                    .red-header-table-general tr:hover { background-color: #f1f1f1; }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    html_report_5 = df_clean_tab5.to_html(index=False, classes="red-header-table-general", escape=False)
                    st.markdown(html_report_5, unsafe_allow_html=True)
                    
                    # Nút Tải file Excel Danh sách Nội dung
                    output5 = io.BytesIO()
                    with pd.ExcelWriter(output5, engine='openpyxl') as writer:
                        df_clean_tab5.to_excel(writer, index=False, sheet_name='Banking_Subject')
                    excel_data5 = output5.getvalue()
                    
                    safe_time_5 = month_input_tab5.replace('/', '_')
                    st.download_button(
                        label="🔽 TẢI BÁO CÁO DANH SÁCH CÚ PHÁP CHUYỂN KHOẢN (EXCEL)",
                        data=excel_data5,
                        file_name=f"Cú_Pháp_Chuyển_Khoản_{safe_time_5}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )

    # ------------ TAB 6: BÁO CÁO LỢI NHUẬN - LOẠI TRỪ CÁ NHÂN ------------
    with tab6:
        st.markdown("### 🏛️ Báo Cáo KQKD Công Ty - Loại Trừ Trạm Cá Nhân")
        st.info("💡 Tab này hoạt động giống Tab 4 nhưng **tự động loại trừ** các trạm được đánh dấu \"Cá nhân\" ở cột Stt trong các Sheet 2, 3, 4, 5 của file dữ liệu.")
        
        with st.form(key='profit_excl_form'):
            st.markdown("**Điền vào Tùy chọn 1 HOẶC Tùy chọn 2, rồi nhấn nút.**")
            time_input_range_t6 = st.text_input("📅 Tùy chọn 1: Khoảng tháng (Ví dụ: 05/2026 - 07/2026):", placeholder="MM/YYYY - MM/YYYY", key="range_t6")
            time_input_t6 = st.text_input("📅 Tùy chọn 2: Một Năm (YYYY) hoặc Một Tháng cụ thể (MM/YYYY):", placeholder="Ví dụ: 03/2026", value=str(datetime.now().year), key="single_t6")
            submit_t6 = st.form_submit_button(label="🔍 TỔNG HỢP LỢI NHUẬN (LOẠI TRỪ CÁ NHÂN)", use_container_width=True)
        
        if submit_t6:
            actual_t6 = time_input_range_t6.strip() if time_input_range_t6.strip() else time_input_t6.strip()
            is_valid_t6 = validate_month_year_or_year(actual_t6)
            f_source = DEFAULT_FILE if DEFAULT_FILE else uploaded_file
            if not is_valid_t6:
                display_error("Bạn đã nhập sai định dạng tháng/năm, vui lòng nhập đúng để hệ thống hiển thị kết quả, xin cám ơn!")
            elif f_source is None:
                st.warning("⚠️ Không tìm thấy File dữ liệu (Upload hoặc Local) để phân tích!")
            else:
                with st.spinner(f"Đang tổng hợp lợi nhuận (loại trừ Cá nhân) cho kỳ {actual_t6}..."):
                    df_t6_raw, excluded_set = get_profit_report_data_exclude_ca_nhan(f_source, actual_t6, df_source)
                
                st.snow()
                st.success(f"🔥 HOÀN TẤT! Lợi nhuận kỳ {actual_t6} đã loại trừ {len(excluded_set)} trạm Cá nhân!")
                
                # Hiển thị danh sách trạm đã loại trừ
                if excluded_set:
                    with st.expander(f"📋 Danh sách {len(excluded_set)} trạm Cá nhân đã loại trừ"):
                        st.write(", ".join(sorted([m.upper() for m in excluded_set])))
                
                # Biểu đồ
                import altair as alt
                st.markdown(f'<h3 style="color:red; font-weight:bold;">📊 Biểu đồ Lợi Nhuận Kỳ {actual_t6} (Đã loại trừ Cá nhân)</h3>', unsafe_allow_html=True)
                
                chart_data_t6 = df_t6_raw.rename(columns={
                    "Tổng Doanh Thu": "Tổng Doanh Thu",
                    "Tiền Chủ Nhà": "Tổng Tiền Trả Chủ Nhà",
                    "Lợi nhuận": "Lợi Nhuận Công Ty"
                }).set_index("Tháng")[["Tổng Doanh Thu", "Tổng Tiền Trả Chủ Nhà", "Lợi Nhuận Công Ty"]]
                
                df_melted_t6 = chart_data_t6.reset_index().melt(
                    id_vars=['Tháng'],
                    value_vars=['Tổng Doanh Thu', 'Tổng Tiền Trả Chủ Nhà', 'Lợi Nhuận Công Ty'],
                    var_name='Chỉ Tiêu',
                    value_name='Số Tiền (VNĐ)'
                )
                
                chart_t6 = alt.Chart(df_melted_t6).mark_bar(size=18).encode(
                    x=alt.X('Chỉ Tiêu:N', axis=alt.Axis(title=None, labels=False, ticks=False), sort=['Tổng Doanh Thu', 'Tổng Tiền Trả Chủ Nhà', 'Lợi Nhuận Công Ty']),
                    y=alt.Y('Số Tiền (VNĐ):Q', title='Giá Trị (VNĐ)'),
                    color=alt.Color('Chỉ Tiêu:N', scale=alt.Scale(
                        domain=['Tổng Doanh Thu', 'Tổng Tiền Trả Chủ Nhà', 'Lợi Nhuận Công Ty'],
                        range=['#FF0000', '#800080', '#00008B']
                    ), legend=alt.Legend(orient='top', title=None)),
                    column=alt.Column('Tháng:N', header=alt.Header(title=None, labelOrient='bottom', labelAlign='center'))
                ).properties(
                    width=75 # Ép chiều rộng mỗi tháng là 75 pixel để cột luôn nhỏ xinh
                ).configure_view(
                    stroke='transparent'
                )
                
                st.altair_chart(chart_t6, use_container_width=False)
                
                # Bảng
                st.markdown(f'<h3 style="color:red; font-weight:bold;">📑 Bảng Tổng Hợp Dòng Tiền Kỳ {actual_t6} (Đã loại trừ Cá nhân)</h3>', unsafe_allow_html=True)
                df_t6_display = df_t6_raw.copy()
                
                # Hàng tổng cộng
                sum_row_t6 = {
                    "Tháng": "TỔNG CỘNG",
                    "Doanh thu Viettel": df_t6_display["Doanh thu Viettel"].sum(),
                    "Doanh thu Vina":    df_t6_display["Doanh thu Vina"].sum(),
                    "Doanh thu Mobi":    df_t6_display["Doanh thu Mobi"].sum(),
                    "Tổng Doanh Thu":    df_t6_display["Tổng Doanh Thu"].sum(),
                    "Tiền Chủ Nhà":      df_t6_display["Tiền Chủ Nhà"].sum(),
                    "Lợi nhuận":         df_t6_display["Lợi nhuận"].sum()
                }
                df_t6_display = pd.concat([pd.DataFrame([sum_row_t6]), df_t6_display], ignore_index=True)
                
                for col in ["Doanh thu Viettel", "Doanh thu Vina", "Doanh thu Mobi", "Tổng Doanh Thu", "Tiền Chủ Nhà", "Lợi nhuận"]:
                    df_t6_display[col] = df_t6_display[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x != 0 else "-")
                
                n_data_t6 = len(df_t6_display) - 1
                df_t6_display.insert(0, 'STT', ["-"] + list(range(1, n_data_t6 + 1)))
                
                df_t6_display.rename(columns={
                    "Tháng": "Tháng mm/yyyy",
                    "Doanh thu Viettel": "Doanh thu Viettel theo tháng",
                    "Doanh thu Vina":    "Doanh thu Vina theo tháng",
                    "Doanh thu Mobi":    "Doanh thu Mobi theo tháng",
                    "Tổng Doanh Thu":    "Sum doanh thu Viettel+Vina+Mobi theo tháng",
                    "Tiền Chủ Nhà":      "Tổng tiền phải trả chủ nhà theo tháng",
                    "Lợi nhuận":         "Lợi nhuận sau thuế của Công ty"
                }, inplace=True)
                
                st.markdown("""
                <style>
                .red-header-tab6 { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                .red-header-tab6 th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 15px; }
                .red-header-tab6 td { border: 1px solid #e0e0e0; padding: 8px; font-size: 14px; }
                .red-header-tab6 tr:nth-child(even) { background-color: #f9f9f9; }
                .red-header-tab6 tr:hover { background-color: #f1f1f1; }
                .red-header-tab6 tbody tr:first-child td { color: #28a745 !important; font-weight: 900 !important; font-size: 1.5em !important; background-color: #e8f5e9 !important; }
                </style>
                """, unsafe_allow_html=True)
                
                html_t6 = df_t6_display.to_html(index=False, classes="red-header-tab6", escape=False)
                st.markdown(html_t6, unsafe_allow_html=True)
                
                out_t6 = io.BytesIO()
                with pd.ExcelWriter(out_t6, engine='openpyxl') as writer:
                    safe_t6 = actual_t6.replace('/', '_').replace(' ', '')
                    df_t6_display.to_excel(writer, index=False, sheet_name=f'LoaiTruCaNhan_{safe_t6}')
                
                st.download_button(
                    label=f"🔽 TẢI XUỐNG BÁO CÁO (LOẠI TRỪ CÁ NHÂN) KỲ {actual_t6} (EXCEL)",
                    data=out_t6.getvalue(),
                    file_name=f"Bao_Cao_LoaiTru_CaNhan_{safe_t6}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

    # ------------ TAB 7: BẢN ĐỒ VỊ TRÍ CÁC TRẠM ------------
    with tab7:
        st.markdown("### 🗺️ Bản Đồ Vị Trí Tất Cả Các Trạm")
        st.info("📍 Bản đồ hiển thị toàn bộ các trạm theo tọa độ. Nhấn vào icon để xem thông tin chi tiết. Màu icon = số nhà mạng tại trạm.")

        try:
            import folium
            from streamlit_folium import st_folium
            folium_available = True
        except ImportError:
            folium_available = False

        if not folium_available:
            st.error("⚠️ Cần cài thêm thư viện bản đồ. Vui lòng chạy lệnh sau rồi khởi động lại app:")
            st.code("pip install folium streamlit-folium", language="bash")
        else:
            # --- CHUẨN BỊ DỮ LIỆU ---
            df_map = df_source.copy()

            # Tìm cột long/lat theo tên linh hoạt
            long_col = None
            lat_col = None
            for c in df_map.columns:
                c_low = str(c).strip().lower()
                if "long" in c_low and long_col is None:
                    long_col = c
                if "lat" in c_low and lat_col is None:
                    lat_col = c

            if long_col is None or lat_col is None:
                st.warning("⚠️ Không tìm thấy cột tọa độ (long/lat) trong dữ liệu. Kiểm tra lại tên cột trong file Excel.")
            else:
                # --- HÀM XÁC ĐỊNH SỐ NHÀ MẠNG ---
                def detect_providers(row):
                    """
                    Viettel: chứa macro/rru/smc => có; Vina/Mobi: chứa 'có' nhưng không có 'không' => có
                    Trả về: (count, [list tên nhà mạng])
                    """
                    providers = []
                    viettel_val = str(row.get("Viettel", "")).strip().lower()
                    if any(kw in viettel_val for kw in ["macro", "rru", "smc"]):
                        providers.append("Viettel")
                    vina_val = str(row.get("Vina", "")).strip().lower()
                    if "có" in vina_val and "không" not in vina_val:
                        providers.append("Vina")
                    mobi_val = str(row.get("Mobi", "")).strip().lower()
                    if "có" in mobi_val and "không" not in mobi_val:
                        providers.append("Mobi")
                    return len(providers), providers

                # Tính số nhà mạng cho mỗi trạm
                provider_results = df_map.apply(lambda r: pd.Series(detect_providers(r)), axis=1)
                df_map["__provider_count__"] = provider_results[0]
                df_map["__providers__"] = provider_results[1]

                # Chuyển đổi tọa độ và lọc hàng hợp lệ
                df_map[long_col] = pd.to_numeric(df_map[long_col], errors='coerce')
                df_map[lat_col]  = pd.to_numeric(df_map[lat_col], errors='coerce')
                df_map_valid = df_map.dropna(subset=[long_col, lat_col]).copy()
                df_map_valid = df_map_valid[
                    (df_map_valid[lat_col].between(-90, 90)) &
                    (df_map_valid[long_col].between(-180, 180))
                ]

                total_on_map = len(df_map_valid)

                # --- BẢNG ĐIỀU KHIỂN: SEARCH + FILTER ---
                ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([3, 2, 1])
                with ctrl_col1:
                    search_ma_tram_map = st.text_input(
                        "🔍 Tìm kiếm mã trạm trên bản đồ:",
                        placeholder="Nhập mã trạm rồi Enter...",
                        key="map_search_input"
                    )
                with ctrl_col2:
                    filter_provider_count = st.selectbox(
                        "📡 Lọc theo số nhà mạng:",
                        options=["🌐 Tất cả trạm", "🔴 Trạm 1 nhà mạng", "🟢 Trạm 2 nhà mạng", "🟣 Trạm 3 nhà mạng"],
                        index=0,
                        key="map_filter_providers"
                    )
                with ctrl_col3:
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.button("🗺️ Áp dụng", use_container_width=True, key="map_apply_btn")

                # --- ÁP DỤNG FILTER SỐ NHÀ MẠNG ---
                if "1 nhà mạng" in filter_provider_count:
                    df_map_filtered = df_map_valid[df_map_valid["__provider_count__"] == 1].copy()
                elif "2 nhà mạng" in filter_provider_count:
                    df_map_filtered = df_map_valid[df_map_valid["__provider_count__"] == 2].copy()
                elif "3 nhà mạng" in filter_provider_count:
                    df_map_filtered = df_map_valid[df_map_valid["__provider_count__"] == 3].copy()
                else:
                    df_map_filtered = df_map_valid.copy()

                filtered_count = len(df_map_filtered)

                # --- THỐNG KÊ 4 METRIC ---
                cnt1 = int((df_map_valid["__provider_count__"] == 1).sum())
                cnt2 = int((df_map_valid["__provider_count__"] == 2).sum())
                cnt3 = int((df_map_valid["__provider_count__"] == 3).sum())
                stat_c1, stat_c2, stat_c3, stat_c4 = st.columns(4)
                stat_c1.metric("📍 Tổng trạm có tọa độ", f"{total_on_map} trạm")
                stat_c2.metric("🔴 Trạm 1 nhà mạng", f"{cnt1} trạm")
                stat_c3.metric("🟢 Trạm 2 nhà mạng", f"{cnt2} trạm")
                stat_c4.metric("🟣 Trạm 3 nhà mạng", f"{cnt3} trạm")

                # --- LEGEND MÀU ---
                st.markdown("""
                <div style='display:flex;gap:20px;align-items:center;padding:8px 14px;
                            background:#f8f9fa;border-radius:8px;margin:6px 0;
                            border:1px solid #e0e0e0;flex-wrap:wrap;'>
                    <span style='font-weight:bold;font-size:13px;'>Chú thích màu:</span>
                    <span style='display:flex;align-items:center;gap:5px;'>
                        <span style='background:#e53935;width:13px;height:13px;border-radius:50%;display:inline-block;'></span>
                        <b style='color:#e53935;font-size:13px;'>Đỏ</b>
                        <span style='font-size:12px;'>= 1 nhà mạng</span>
                    </span>
                    <span style='display:flex;align-items:center;gap:5px;'>
                        <span style='background:#2e7d32;width:13px;height:13px;border-radius:50%;display:inline-block;'></span>
                        <b style='color:#2e7d32;font-size:13px;'>Xanh lá</b>
                        <span style='font-size:12px;'>= 2 nhà mạng</span>
                    </span>
                    <span style='display:flex;align-items:center;gap:5px;'>
                        <span style='background:#6a1b9a;width:13px;height:13px;border-radius:50%;display:inline-block;'></span>
                        <b style='color:#6a1b9a;font-size:13px;'>Tím</b>
                        <span style='font-size:12px;'>= 3 nhà mạng</span>
                    </span>
                    <span style='display:flex;align-items:center;gap:5px;'>
                        <span style='background:#FFD700;width:13px;height:13px;border-radius:50%;border:2px solid #888;display:inline-block;'></span>
                        <span style='font-size:12px;'>Vòng vàng = trạm đang search</span>
                    </span>
                </div>
                """, unsafe_allow_html=True)

                # --- XÁC ĐỊNH TÂM BẢN ĐỒ & SEARCH ---
                center_lat = df_map_valid[lat_col].mean()
                center_lon = df_map_valid[long_col].mean()
                zoom_start = 7
                found_station = None

                if search_ma_tram_map.strip():
                    search_key = search_ma_tram_map.strip().lower()
                    match_rows = df_map_valid[
                        df_map_valid["mã trạm"].astype(str).str.strip().str.lower() == search_key
                    ]
                    if not match_rows.empty:
                        found_station = match_rows.iloc[0]
                        center_lat = float(found_station[lat_col])
                        center_lon = float(found_station[long_col])
                        zoom_start = 15

                        # Thông tin nhà mạng của trạm tìm được
                        f_count, f_providers = detect_providers(found_station)
                        p_label = " | ".join(f_providers) if f_providers else "Không xác định"
                        pcolor_map = {0: "#9e9e9e", 1: "#e53935", 2: "#2e7d32", 3: "#6a1b9a"}
                        p_color = pcolor_map.get(f_count, "#9e9e9e")

                        # Badge từng nhà mạng
                        badge_colors = {"Viettel": "#e53935", "Vina": "#1565c0", "Mobi": "#e65100"}
                        nm_badges = "".join([
                            f"<span style='background:{badge_colors.get(p,'#555')};color:white;"
                            f"padding:2px 10px;border-radius:10px;font-size:12px;"
                            f"font-weight:bold;margin-right:5px;'>{p}</span>"
                            for p in f_providers
                        ]) or "<span style='color:#999;'>Chưa có nhà mạng</span>"

                        st.markdown(f"""
                        <div style='background:linear-gradient(135deg,#e8f5e9,#f3e5f5);
                                    border-left:5px solid {p_color};border-radius:8px;
                                    padding:12px 18px;margin:8px 0;'>
                            <div style='font-size:16px;font-weight:bold;color:{p_color};margin-bottom:8px;'>
                                🎯 Tìm thấy trạm: {found_station['mã trạm']}
                            </div>
                            <div style='display:flex;gap:24px;flex-wrap:wrap;align-items:center;font-size:13px;'>
                                <span>📌 <b>Tọa độ:</b> {center_lat:.6f}, {center_lon:.6f}</span>
                                <span>📶 <b>Số nhà mạng:</b>
                                    <span style='background:{p_color};color:white;padding:1px 10px;
                                                border-radius:12px;font-weight:bold;margin-left:4px;'>
                                        {f_count} nhà mạng
                                    </span>
                                </span>
                                <span>🏢 <b>Nhà mạng:</b> {nm_badges}</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.warning(f"❌ Không tìm thấy mã trạm **'{search_ma_tram_map.strip()}'** trong dữ liệu.")

                # Thông báo số trạm sau filter
                if filtered_count < total_on_map:
                    st.info(f"🔽 Đang hiển thị **{filtered_count}** trạm (lọc: {filter_provider_count})")
                else:
                    st.success(f"✅ Hiển thị tất cả **{total_on_map}** trạm có tọa độ.")

                # --- TẠO BẢN ĐỒ FOLIUM ---
                m = folium.Map(
                    location=[center_lat, center_lon],
                    zoom_start=zoom_start,
                    tiles="OpenStreetMap"
                )
                folium.TileLayer(
                    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                    attr="Esri",
                    name="🛰️ Vệ Tinh (Esri)",
                    overlay=False,
                    control=True
                ).add_to(m)
                folium.TileLayer(
                    tiles="OpenStreetMap",
                    name="🗺️ Bản Đồ Đường (OSM)",
                    overlay=False,
                    control=True
                ).add_to(m)
                folium.LayerControl(position="topright").add_to(m)

                # --- MÀU ICON THEO SỐ NHÀ MẠNG ---
                icon_color_map = {0: "#9e9e9e", 1: "#e53935", 2: "#2e7d32", 3: "#6a1b9a"}

                # --- CỘT POPUP ---
                display_cols_for_popup = [
                    c for c in DISPLAY_COLUMNS
                    if c in df_map_filtered.columns and c not in ["__raw_amount__", "__is_due_this_month__"]
                ]

                # --- VẼ MARKER ---
                for _, row_m in df_map_filtered.iterrows():
                    ma_tram_val = str(row_m.get("mã trạm", "")).strip()
                    lat_val = float(row_m[lat_col])
                    lon_val = float(row_m[long_col])

                    p_count, p_list = detect_providers(row_m)
                    icon_color = icon_color_map.get(p_count, "#9e9e9e")
                    p_names = " | ".join(p_list) if p_list else "Không có"

                    # Badge nhà mạng trong popup
                    badge_colors = {"Viettel": "#e53935", "Vina": "#1565c0", "Mobi": "#e65100"}
                    nm_badge_html = "".join([
                        f"<span style='background:{badge_colors.get(pn,'#555')};color:white;"
                        f"padding:2px 8px;border-radius:10px;font-size:11px;"
                        f"font-weight:bold;margin-right:4px;'>{pn}</span>"
                        for pn in p_list
                    ]) or "<span style='color:#999;font-size:11px;'>Chưa có nhà mạng</span>"

                    # Popup HTML
                    popup_rows_html = ""
                    for col_p in display_cols_for_popup:
                        val_p = row_m.get(col_p, "-")
                        if pd.isna(val_p) or str(val_p).strip() == "": val_p = "-"
                        color_style = "color:#1565c0;" if col_p in EXTRA_PAY_COLS else "color:#333;"
                        popup_rows_html += (
                            f"<tr>"
                            f"<td style='font-weight:bold;white-space:nowrap;padding:4px 8px;"
                            f"border-bottom:1px solid #eee;font-size:12px;{color_style}'>{col_p}</td>"
                            f"<td style='padding:4px 8px;border-bottom:1px solid #eee;font-size:12px;'>{val_p}</td>"
                            f"</tr>"
                        )

                    popup_html = (
                        f"<div style='min-width:320px;max-width:440px;max-height:480px;"
                        f"overflow-y:auto;font-family:Arial,sans-serif;'>"
                        f"<div style='background:{icon_color};color:white;padding:8px 12px;"
                        f"border-radius:6px 6px 0 0;font-weight:bold;font-size:14px;'>"
                        f"📡 Trạm: {ma_tram_val}</div>"
                        f"<div style='background:#f5f5f5;padding:6px 12px;"
                        f"border-bottom:2px solid {icon_color};'>"
                        f"<span style='font-size:12px;font-weight:bold;color:#555;margin-right:6px;'>📶 Nhà mạng:</span>"
                        f"{nm_badge_html}</div>"
                        f"<table style='width:100%;border-collapse:collapse;background:white;'>"
                        f"{popup_rows_html}</table></div>"
                    )

                    # DivIcon màu động
                    icon = folium.DivIcon(
                        html=(
                            f"<div style='width:100px;display:flex;flex-direction:column;align-items:center;'>"
                            f"<div style='background:{icon_color};color:white;font-size:10px;"
                            f"font-weight:bold;padding:2px 6px;border-radius:4px;white-space:nowrap;"
                            f"box-shadow:0 1px 4px rgba(0,0,0,0.45);margin-bottom:2px;"
                            f"line-height:16px;'>{ma_tram_val}</div>"
                            f"<svg width='18' height='24' viewBox='0 0 18 24' xmlns='http://www.w3.org/2000/svg'>"
                            f"<path d='M9 0C5.13 0 2 3.13 2 7c0 5.25 7 17 7 17s7-11.75 7-17c0-3.87-3.13-7-7-7z'"
                            f" fill='{icon_color}'/>"
                            f"<circle cx='9' cy='7' r='3' fill='white'/></svg></div>"
                        ),
                        icon_size=(100, 44),
                        icon_anchor=(50, 44)
                    )

                    # Vòng vàng cho trạm search
                    if found_station is not None and ma_tram_val.lower() == search_ma_tram_map.strip().lower():
                        folium.CircleMarker(
                            location=[lat_val, lon_val],
                            radius=26,
                            color="#FFD700",
                            fill=True,
                            fill_color="#FFD700",
                            fill_opacity=0.3,
                            weight=4
                        ).add_to(m)

                    folium.Marker(
                        location=[lat_val, lon_val],
                        popup=folium.Popup(popup_html, max_width=460),
                        tooltip=f"📡 {ma_tram_val}  |  📶 {p_names}",
                        icon=icon
                    ).add_to(m)

                # --- HIỂN THỊ BẢN ĐỒ ---
                st.markdown("**💡 Ghi chú:** Nhấn icon để xem chi tiết. Hover để xem nhanh nhà mạng. Nút góc phải để đổi lớp bản đồ.")
                st_fo                        # Lọc theo mã trạm (hỗ trợ nhiều mã trạm cách nhau bằng dấu phẩy)
                        if ma_tram_t8.strip():
                            # Tách theo phẩy hoặc xuống dòng
                            raw_keys = ma_tram_t8.replace(',', '\n').split('\n')
                            search_keys_t8 = [s.strip().lower() for s in raw_keys if s.strip()]
                            if search_keys_t8:
                                mask_t8 = df_s6[ma_col_t8].astype(str).str.strip().str.lower().isin(search_keys_t8)
                                df_s6_filtered = df_s6[mask_t8].copy()
                            else:
                                df_s6_filtered = df_s6.copy()
                        else:
                            df_s6_filtered = df_s6.copy()

                        if df_s6_filtered.empty:
                            st.warning("❌ Không tìm thấy mã trạm nào khớp!")
                        else:
                            # --- TÍNH TỔNG CỘNG TRƯỚC KHI FORMAT ---
                            summary_row = {ma_col_t8: "TỔNG CỘNG"}
                            
                            # Xác định các cột số để tính tổng
                            for c in df_s6_filtered.columns:
                                if c == ma_col_t8: continue
                                # Thử convert sang float
                                try:
                                    # Tạo bản sao seri để tính toán
                                    temp_s = pd.to_numeric(df_s6_filtered[c], errors='coerce')
                                    if not temp_s.isna().all():
                                        total_val = temp_s.sum()
                                        summary_row[c] = total_val
                                    else:
                                        summary_row[c] = "-"
                                except:
                                    summary_row[c] = "-"

                            # Format các cột số trong DataFrame chính
                            for c in df_s6_filtered.columns:
                                if c == ma_col_t8: continue
                                try:
                                    # Convert sang số nếu có thể
                                    df_s6_filtered[c] = pd.to_numeric(df_s6_filtered[c], errors='coerce')
                                    df_s6_filtered[c] = df_s6_filtered[c].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
                                except:
                                    pass

                            # Format các giá trị trong hàng tổng cộng
                            formatted_summary = {}
                            for k, v in summary_row.items():
                                if isinstance(v, (int, float)):
                                    formatted_summary[k] = f"{v:,.0f}"
                                else:
                                    formatted_summary[k] = v

                            st.success(f"✅ Tìm thấy **{len(df_s6_filtered)}** trạm.")

                            # --- BẢNG 1: LƯỚI NGANG ---
                            st.markdown('<h3 style="color:red; font-weight:bold;">📊 Bảng 1: Tổng Hợp Lưới Ngang</h3>', unsafe_allow_html=True)
                            
                            # Thêm hàng tổng cộng vào bảng hiển thị
                            df_t8_display = df_s6_filtered.copy()
                            # Chèn STT
                            df_t8_display.insert(0, 'STT', range(1, len(df_t8_display) + 1))
                            
                            # Tạo hàng tổng cộng cho bảng hiển thị (STT là "-")
                            final_summary_row = {"STT": "-"}
                            final_summary_row.update(formatted_summary)
                            
                            # Kết hợp
                            df_final_t8 = pd.concat([df_t8_display, pd.DataFrame([final_summary_row])], ignore_index=True)

                            # CSS cho bảng và hàng tổng cộng
                            st.markdown("""
                            <style>
                            .hv-table { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                            .hv-table th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 14px; white-space: nowrap; }
                            .hv-table td { border: 1px solid #e0e0e0; padding: 8px; font-size: 13px; }
                            .hv-table tr:nth-child(even) { background-color: #f9f9f9; }
                            .hv-table tr:hover { background-color: #fff3f3; }
                            /* Style cho hàng cuối (Tổng cộng) */
                            .hv-table tr:last-child { background-color: #e3f2fd !important; font-weight: bold; color: #0d47a1; }
                            .hv-table tr:last-child td { color: #0d47a1 !important; font-size: 15px; }
                            </style>
                            """, unsafe_allow_html=True)
                            
                            html_t8 = df_final_t8.to_html(index=False, classes="hv-table", escape=False)
                            st.markdown(html_t8, unsafe_allow_html=True)�ến cuối). Nhập mã trạm để tra cứu, hoặc để trống để xem toàn bộ.")

        with st.form(key='hoan_von_search_form'):
            ma_tram_t8 = st.text_input("🔍 Nhập mã trạm cần tìm (để trống = xem toàn bộ):", placeholder="Ví dụ: HCM001")
            submit_t8 = st.form_submit_button(label="🔍 TÌM KIẾM", use_container_width=True)

        if submit_t8:
            f_source_t8 = DEFAULT_FILE if DEFAULT_FILE else uploaded_file
            if f_source_t8 is None:
                st.warning("⚠️ Không tìm thấy File dữ liệu!")
            else:
                try:
                    if hasattr(f_source_t8, 'seek'):
                        f_source_t8.seek(0)
                    xl_t8 = pd.ExcelFile(f_source_t8)
                    sheet6_name = next((s for s in xl_t8.sheet_names if 'sheet 6' in s.lower() or 'hoàn vốn' in s.lower() or 'hoan von' in s.lower()), None)

                    if not sheet6_name:
                        st.error("⚠️ Không tìm thấy sheet 'Sheet 6_time hoàn vốn' trong file Excel!")
                    else:
                        if hasattr(f_source_t8, 'seek'):
                            f_source_t8.seek(0)
                        # Đọc sheet, dùng header mặc định
                        df_s6_raw = pd.read_excel(f_source_t8, sheet_name=sheet6_name, header=0)

                        # Lấy từ cột C (index 2) trở đi
                        if len(df_s6_raw.columns) > 2:
                            df_s6 = df_s6_raw.iloc[:, 2:].copy()
                        else:
                            df_s6 = df_s6_raw.copy()

                        # Loại bỏ dòng rỗng (dựa trên cột đầu tiên của subset)
                        first_col = df_s6.columns[0]
                        df_s6 = df_s6[df_s6[first_col].notna()]
                        df_s6 = df_s6[df_s6[first_col].astype(str).str.strip() != '']
                        df_s6 = df_s6[~df_s6[first_col].astype(str).str.lower().isin(['nan', 'null', 'mã trạm', 'stt', 'mã'])]
                        df_s6.reset_index(drop=True, inplace=True)

                        # Format số cho dễ đọc
                        for c in df_s6.columns:
                            if pd.api.types.is_float_dtype(df_s6[c]):
                                df_s6[c] = df_s6[c].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "-")
                            elif pd.api.types.is_datetime64_any_dtype(df_s6[c]):
                                df_s6[c] = df_s6[c].dt.strftime('%m/%d/%Y')

                        df_s6 = df_s6.fillna("-")

                        # Đổi tên cột đầu tiên thành "mã trạm" nếu chưa có
                        col_names = list(df_s6.columns)
                        ma_col_t8 = col_names[0]

                        # Lọc theo mã trạm
                        if ma_tram_t8.strip():
                            search_keys_t8 = [s.strip().lower() for s in ma_tram_t8.replace(',', '\n').split('\n') if s.strip()]
                            mask_t8 = df_s6[ma_col_t8].astype(str).str.strip().str.lower().isin(search_keys_t8)
                            df_s6_filtered = df_s6[mask_t8]
                        else:
                            df_s6_filtered = df_s6

                        if df_s6_filtered.empty:
                            st.warning("❌ Không tìm thấy mã trạm nào khớp!")
                        else:
                            st.success(f"✅ Tìm thấy **{len(df_s6_filtered)}** trạm.")

                            # --- BẢNG 1: LƯỚI NGANG ---
                            st.markdown('<h3 style="color:red; font-weight:bold;">📊 Bảng 1: Tổng Hợp Lưới Ngang</h3>', unsafe_allow_html=True)
                            st.markdown("""
                            <style>
                            .hv-table { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; font-family: "Source Sans Pro", sans-serif; }
                            .hv-table th { background-color: #ffeaea !important; color: #ff0000 !important; font-weight: 900 !important; border: 1px solid #e0e0e0; padding: 10px; text-align: left; font-size: 14px; white-space: nowrap; }
                            .hv-table td { border: 1px solid #e0e0e0; padding: 8px; font-size: 13px; }
                            .hv-table tr:nth-child(even) { background-color: #f9f9f9; }
                            .hv-table tr:hover { background-color: #fff3f3; }
                            </style>
                            """, unsafe_allow_html=True)
                            df_t8_show = df_s6_filtered.copy()
                            df_t8_show.insert(0, 'STT', range(1, len(df_t8_show) + 1))
                            html_t8 = df_t8_show.to_html(index=False, classes="hv-table", escape=False)
                            st.markdown(html_t8, unsafe_allow_html=True)

                            # --- BẢNG 2: DẠNG THẺ DỌC ---
                            st.markdown("---")
                            st.markdown("### 🏷️ Bảng 2: Chi Tiết Dạng Thẻ (Đọc Dọc Trên Điện Thoại)")
                            if len(df_s6_filtered) > 50:
                                st.warning("⚠️ Hiển thị thẻ cho 50 trạm đầu để tránh chậm máy.")
                                df_cards_t8 = df_s6_filtered.head(50)
                            else:
                                df_cards_t8 = df_s6_filtered

                            for _, row_t8 in df_cards_t8.iterrows():
                                tram_id_t8 = str(row_t8.iloc[0]) if str(row_t8.iloc[0]) != "" else "Không Mẫu"
                                with st.expander(f"⏱️ Hoàn Vốn Trạm: {tram_id_t8}", expanded=True):
                                    for col_t8 in df_s6_filtered.columns:
                                        val_t8 = row_t8[col_t8]
                                        if pd.isna(val_t8) or str(val_t8).strip() in ("", "-", "nan"): val_t8 = "-"
                                        st.markdown(f"**{col_t8}:** &nbsp;&nbsp; {val_t8}")

                except Exception as e_t8:
                    st.error(f"⚠️ Lỗi khi đọc sheet hoàn vốn: {e_t8}")

    # ============================================================
    # ------------ TAB 9: TÍNH THỜI GIAN HOÀN VỐN ---------------
    # ============================================================
    with tab9:
        st.markdown("### 🧮 Tính Thời Gian Hoàn Vốn")
        st.info("💡 Nhập mã trạm để tự động điền dữ liệu có sẵn. Sau đó nhập các thông số còn lại để tính số tháng hoàn vốn.")

        # ---- Helper Functions ----
        def parse_num_t9(val):
            if val is None or (isinstance(val, float) and pd.isna(val)): return 0.0
            try:
                if isinstance(val, (int, float)): return float(val)
                s = str(val).strip().replace(',', '')
                return float(s) if s else 0.0
            except:
                return 0.0

        def load_sheet6_for_station(f_src, ma_tram_key):
            """Tìm dòng data của mã trạm trong Sheet 6, trả về dict các cột."""
            try:
                if hasattr(f_src, 'seek'): f_src.seek(0)
                xl = pd.ExcelFile(f_src)
                sheet6 = next((s for s in xl.sheet_names if 'sheet 6' in s.lower() or 'hoàn vốn' in s.lower() or 'hoan von' in s.lower()), None)
                if not sheet6: return None
                if hasattr(f_src, 'seek'): f_src.seek(0)
                df6 = pd.read_excel(f_src, sheet_name=sheet6, header=0)
                # Cột mã trạm (cột C, index 2)
                if len(df6.columns) < 3: return None
                ma_col = df6.columns[2]
                row_match = df6[df6[ma_col].astype(str).str.strip().str.lower() == ma_tram_key.lower()]
                if row_match.empty: return None
                return row_match.iloc[0].to_dict()
            except:
                return None

        def get_rent_from_landlord_sheet(f_src, ma_tram_key):
            """Lấy Số tiền thuê/tháng từ sheet 'Theo dõi thanh toán chủ nhà' theo mã trạm."""
            try:
                if hasattr(f_src, 'seek'): f_src.seek(0)
                xl = pd.ExcelFile(f_src)
                sheet_ll = next((s for s in xl.sheet_names if 'theo dõi thanh toán' in s.lower() or 'thanh toán chủ nhà' in s.lower()), None)
                if not sheet_ll: return 0.0
                if hasattr(f_src, 'seek'): f_src.seek(0)
                df_ll = pd.read_excel(f_src, sheet_name=sheet_ll, header=0)
                if df_ll.empty: return 0.0
                # Tìm cột mã trạm
                ma_col_ll = None
                for c in df_ll.columns:
                    if 'mã trạm' in str(c).lower() or ('mã' in str(c).lower() and 'trạm' in str(c).lower()):
                        ma_col_ll = c; break
                if not ma_col_ll:
                    ma_col_ll = df_ll.columns[0]
                # Tìm cột số tiền thuê/tháng
                amount_col_ll = None
                for c in df_ll.columns:
                    c_low = str(c).lower()
                    if 'thuê/tháng' in c_low or 'tiền thuê' in c_low or 'số tiền thuê' in c_low:
                        amount_col_ll = c; break
                if not amount_col_ll:
                    for c in df_ll.columns:
                        if 'số tiền' in str(c).lower():
                            amount_col_ll = c; break
                if not amount_col_ll: return 0.0
                row_ll = df_ll[df_ll[ma_col_ll].astype(str).str.strip().str.lower() == ma_tram_key.lower()]
                if row_ll.empty: return 0.0
                return parse_num_t9(row_ll.iloc[0][amount_col_ll])
            except:
                return 0.0

        def get_vina_monthly_from_sheet(f_src, ma_tram_key):
            """Lấy tiền Vina trả/tháng từ sheet Vina."""
            try:
                if hasattr(f_src, 'seek'): f_src.seek(0)
                xl = pd.ExcelFile(f_src)
                vina_sheet = next((s for s in xl.sheet_names if 'vina' in s.lower()), None)
                if not vina_sheet: return 0.0
                if hasattr(f_src, 'seek'): f_src.seek(0)
                df_vina = pd.read_excel(f_src, sheet_name=vina_sheet, header=0)
                if df_vina.empty: return 0.0
                ma_col_v = df_vina.columns[0]
                for c in df_vina.columns:
                    if 'mã' in str(c).lower(): ma_col_v = c; break
                monthly_col_v = None
                kw_list = ['trả/tháng', 'thuê/tháng', 'giá thuê', 'đơn giá', 'mức cước', 'số tiền', 'giá']
                for kw in kw_list:
                    for c in df_vina.columns:
                        if kw in str(c).lower() and c != ma_col_v:
                            monthly_col_v = c; break
                    if monthly_col_v: break
                if not monthly_col_v: return 0.0
                row_v = df_vina[df_vina[ma_col_v].astype(str).str.strip().str.lower() == ma_tram_key.lower()]
                if row_v.empty: return 0.0
                return parse_num_t9(row_v.iloc[0][monthly_col_v])
            except:
                return 0.0

        def get_mobi_monthly_from_sheet(f_src, ma_tram_key):
            """Lấy tiền Mobi trả/tháng từ sheet Mobi."""
            try:
                if hasattr(f_src, 'seek'): f_src.seek(0)
                xl = pd.ExcelFile(f_src)
                mobi_sheet = next((s for s in xl.sheet_names if 'mobi' in s.lower()), None)
                if not mobi_sheet: return 0.0
                if hasattr(f_src, 'seek'): f_src.seek(0)
                df_mobi = pd.read_excel(f_src, sheet_name=mobi_sheet, header=0)
                if df_mobi.empty: return 0.0
                ma_col_m = df_mobi.columns[0]
                for c in df_mobi.columns:
                    if 'mã' in str(c).lower(): ma_col_m = c; break
                monthly_col_m = None
                kw_list = ['trả/tháng', 'thuê/tháng', 'giá thuê', 'đơn giá', 'mức cước', 'số tiền', 'giá']
                for kw in kw_list:
                    for c in df_mobi.columns:
                        if kw in str(c).lower() and c != ma_col_m:
                            monthly_col_m = c; break
                    if monthly_col_m: break
                if not monthly_col_m: return 0.0
                row_m = df_mobi[df_mobi[ma_col_m].astype(str).str.strip().str.lower() == ma_tram_key.lower()]
                if row_m.empty: return 0.0
                return parse_num_t9(row_m.iloc[0][monthly_col_m])
            except:
                return 0.0

        # ---- UI Tab 9 ----
        f_source_t9 = DEFAULT_FILE if DEFAULT_FILE else (uploaded_file if 'uploaded_file' in dir() else None)

        st.markdown("#### 🔎 Bước 1: Nhập Mã Trạm")
        col_ma, col_lookup = st.columns([3, 1])
        with col_ma:
            ma_tram_t9 = st.text_input("Ô 1 - Mã Trạm:", placeholder="Ví dụ: HCM001", key="t9_ma_tram")
        with col_lookup:
            st.markdown("<br>", unsafe_allow_html=True)
            lookup_btn = st.button("🔍 Tra cứu", key="t9_lookup_btn", use_container_width=True)

        # State cho auto-fill
        if 't9_data' not in st.session_state:
            st.session_state['t9_data'] = {}

        if lookup_btn and ma_tram_t9.strip() and f_source_t9:
            row6 = load_sheet6_for_station(f_source_t9, ma_tram_t9.strip())
            vina_monthly_auto = get_vina_monthly_from_sheet(f_source_t9, ma_tram_t9.strip())
            mobi_monthly_auto = get_mobi_monthly_from_sheet(f_source_t9, ma_tram_t9.strip())
            rent_auto = get_rent_from_landlord_sheet(f_source_t9, ma_tram_t9.strip())

            if row6 is not None:
                # Tìm các cột theo keyword
                cols_list = list(row6.keys())
                def find_col_val(keywords):
                    for kw in keywords:
                        for k in cols_list:
                            if kw.lower() in str(k).lower():
                                return parse_num_t9(row6[k])
                    return 0.0

                tong_cp_sau_vat = find_col_val(['tổng chi phí sau vat', 'tổng cp sau vat', 'chi phí sau vat', 'tổng chi phí'])
                st.session_state['t9_data'] = {
                    'found': True,
                    'tong_cp_sau_vat': tong_cp_sau_vat,
                    'vina_monthly': vina_monthly_auto,
                    'mobi_monthly': mobi_monthly_auto,
                    'rent': rent_auto,
                    'ma_tram': ma_tram_t9.strip()
                }
                st.success(f"✅ Tìm thấy dữ liệu trạm **{ma_tram_t9.strip()}** trong Sheet 6. Đã tự động điền các trường.")
            else:
                st.session_state['t9_data'] = {
                    'found': False,
                    'vina_monthly': vina_monthly_auto,
                    'mobi_monthly': mobi_monthly_auto,
                    'rent': rent_auto,
                    'ma_tram': ma_tram_t9.strip()
                }
                st.warning(f"⚠️ Không tìm thấy mã trạm **{ma_tram_t9.strip()}** trong Sheet 6. Vui lòng nhập thủ công 'Tổng Chi Phí Sau VAT'.")

        t9 = st.session_state.get('t9_data', {})
        found_t9 = t9.get('found', False)
        prefill_tcp = t9.get('tong_cp_sau_vat', 0.0)
        prefill_vina = t9.get('vina_monthly', 0.0)
        prefill_mobi = t9.get('mobi_monthly', 0.0)
        prefill_rent = t9.get('rent', 0.0)

        st.markdown("---")
        st.markdown("#### 📝 Bước 2: Nhập Thông Số Tài Chính")

        # Ô 2: Tổng Chi Phí Sau VAT
        st.markdown("**Ô 2 - Tổng Chi Phí Sau VAT (VNĐ):**")
        if found_t9 and prefill_tcp > 0:
            st.info(f"✅ Auto điền từ Sheet 6: **{prefill_tcp:,.0f}** VNĐ")
            tcp_val = st.number_input("", min_value=0.0, value=float(prefill_tcp), step=1000000.0, format="%.0f", key="t9_tcp", label_visibility="collapsed")
        else:
            if t9.get('ma_tram'):
                st.warning("✋ Mã trạm không có trong Sheet 6 — vui lòng nhập tay:")
            tcp_val = st.number_input("", min_value=0.0, value=0.0, step=1000000.0, format="%.0f", key="t9_tcp", label_visibility="collapsed")

        # Ô 3: Chi phí lãi vay (= F4 = Tổng CP Sau VAT * 12% / 12 * 24)
        lai_vay_f = tcp_val * 0.12 / 12 * 24 if tcp_val > 0 else 0.0
        st.markdown(f"**Ô 3 - Chi Phí Lãi Vay** *(= Tổng CP Sau VAT × 12% / 12 × 24 tháng)*:")
        st.markdown(f"<div style='background:#eef6ff;border-left:4px solid #1565c0;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🔵 <b>{lai_vay_f:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        # Ô 4: Tổng chi phí thực bỏ ra (có tính lãi vay) (= G4 = Tổng CP Sau VAT + Chi phí lãi vay)
        tong_cp_thuc = tcp_val + lai_vay_f
        st.markdown(f"**Ô 4 - Tổng Chi Phí Thực Bỏ Ra (Có Tính Lãi Vay)** *(= Tổng CP Sau VAT + Lãi Vay)*:")
        st.markdown(f"<div style='background:#eef6ff;border-left:4px solid #1565c0;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🔵 <b>{tong_cp_thuc:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        # Ô 5: Chi phí feedback lại Vina
        st.markdown("**Ô 5 - Chi Phí Feedback Lại Vina:**")
        col5a, col5b_label = st.columns([2, 3])
        with col5a:
            thang_fb_vina = st.number_input("Ô 5.1 - Số tháng Feedback Vina:", min_value=0, value=0, step=1, key="t9_thang_fb_vina")
        if prefill_vina > 0:
            st.caption(f"💡 Tiền Vina trả/tháng tự động: **{prefill_vina:,.0f}** VNĐ")
            vina_monthly_t9 = st.number_input("Tiền Vina/tháng (VNĐ):", min_value=0.0, value=float(prefill_vina), step=100000.0, format="%.0f", key="t9_vina_monthly")
        else:
            vina_monthly_t9 = st.number_input("Tiền Vina/tháng (VNĐ):", min_value=0.0, value=0.0, step=100000.0, format="%.0f", key="t9_vina_monthly")
        # Ô 5.2 = Tiền Vina/tháng × Số tháng Feedback Vina / 1.1
        fb_vina_val = vina_monthly_t9 * thang_fb_vina / 1.1 if thang_fb_vina > 0 else 0.0
        st.markdown(f"Ô 5.2 - **Chi Phí Feedback Vina** *(= {vina_monthly_t9:,.0f} × {thang_fb_vina} / 1.1)*:")
        st.markdown(f"<div style='background:#e8f5e9;border-left:4px solid #2e7d32;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🟢 <b>{fb_vina_val:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        # Ô 6: Chi phí feedback lại Mobi
        st.markdown("**Ô 6 - Chi Phí Feedback Lại Mobi:**")
        thang_fb_mobi = st.number_input("Ô 6.1 - Số tháng Feedback Mobi:", min_value=0, value=0, step=1, key="t9_thang_fb_mobi")
        if prefill_mobi > 0:
            st.caption(f"💡 Tiền Mobi trả/tháng tự động: **{prefill_mobi:,.0f}** VNĐ")
            mobi_monthly_t9 = st.number_input("Tiền Mobi/tháng (VNĐ):", min_value=0.0, value=float(prefill_mobi), step=100000.0, format="%.0f", key="t9_mobi_monthly")
        else:
            mobi_monthly_t9 = st.number_input("Tiền Mobi/tháng (VNĐ):", min_value=0.0, value=0.0, step=100000.0, format="%.0f", key="t9_mobi_monthly")
        fb_mobi_val = mobi_monthly_t9 * thang_fb_mobi / 1.1 if thang_fb_mobi > 0 else 0.0
        st.markdown(f"Ô 6.2 - **Chi Phí Feedback Mobi** *(= {mobi_monthly_t9:,.0f} × {thang_fb_mobi} / 1.1)*:")
        st.markdown(f"<div style='background:#e8f5e9;border-left:4px solid #2e7d32;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🟢 <b>{fb_mobi_val:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        # Ô 7: Tổng chi phí bỏ ra (= J4 = G4 + Feedback Vina + Feedback Mobi)
        tong_cp_bo_ra = tong_cp_thuc + fb_vina_val + fb_mobi_val
        st.markdown("**Ô 7 - Tổng Chi Phí Bỏ Ra** *(= Tổng CP Thực + Feedback Vina + Feedback Mobi)*:")
        st.markdown(f"<div style='background:#fff3e0;border-left:4px solid #e65100;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🟠 <b>{tong_cp_bo_ra:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("#### 💰 Bước 3: Thông Tin Thu Nhập Hàng Tháng")

        # Ô 8: Tiền trả chủ nhà/tháng - auto từ sheet TDTTCN
        st.markdown("**Ô 8 - Tiền Trả Chủ Nhà/Tháng:**")
        if prefill_rent > 0:
            st.caption(f"💡 Tự động từ Sheet 'Theo dõi thanh toán chủ nhà': **{prefill_rent:,.0f}** VNĐ/tháng")
            rent_t9 = st.number_input("Tiền thuê chủ nhà/tháng (VNĐ):", min_value=0.0, value=float(prefill_rent), step=100000.0, format="%.0f", key="t9_rent")
        else:
            rent_t9 = st.number_input("Tiền thuê chủ nhà/tháng (VNĐ):", min_value=0.0, value=0.0, step=100000.0, format="%.0f", key="t9_rent")

        # Ô 9: Tiền Viettel trả/tháng
        viettel_monthly_t9 = st.number_input("**Ô 9 - Tiền Nhà Mạng Viettel Trả/Tháng (VNĐ):**", min_value=0.0, value=0.0, step=100000.0, format="%.0f", key="t9_viettel")

        # Ô 10 Vina (hiển thị lại với label khác)
        st.markdown(f"**Ô 10a - Tiền Nhà Mạng Vina Trả/Tháng:** → Sử dụng giá trị đã nhập ở Ô 5 = **{vina_monthly_t9:,.0f}** VNĐ")

        # Ô 10b: Mobi
        st.markdown(f"**Ô 10b - Tiền Nhà Mạng Mobi Trả/Tháng:** → Sử dụng giá trị đã nhập ở Ô 6 = **{mobi_monthly_t9:,.0f}** VNĐ")

        st.markdown("---")
        st.markdown("#### 📊 Kết Quả Tính Toán")

        # Ô 11: Tổng thu/tháng (gồm VAT) = O4 = Viettel + Vina + Mobi
        tong_thu_incl_vat = viettel_monthly_t9 + vina_monthly_t9 + mobi_monthly_t9
        st.markdown(f"**Ô 11 - Tổng Thu/Tháng (Gồm VAT)** *(= Viettel + Vina + Mobi)*:")
        st.markdown(f"<div style='background:#f3e5f5;border-left:4px solid #6a1b9a;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🟣 <b>{tong_thu_incl_vat:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        # Ô 12: Tổng thu/tháng sau khi trừ thuế 10% = P4 = O4 / 1.1
        tong_thu_after_tax = tong_thu_incl_vat / 1.1 if tong_thu_incl_vat > 0 else 0.0
        st.markdown(f"**Ô 12 - Tổng Thu/Tháng Sau Khi Trừ Thuế 10%** *(= Tổng Thu / 1.1)*:")
        st.markdown(f"<div style='background:#f3e5f5;border-left:4px solid #6a1b9a;padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"🟣 <b>{tong_thu_after_tax:,.0f}</b> VNĐ</div>", unsafe_allow_html=True)

        # Ô 13: Delta, lãi ròng/tháng = Q4 = Tổng thu sau thuế - Tiền chủ nhà
        delta_lai_rong = tong_thu_after_tax - rent_t9
        delta_color = "#2e7d32" if delta_lai_rong >= 0 else "#c62828"
        delta_icon = "📈" if delta_lai_rong >= 0 else "📉"
        st.markdown(f"**Ô 13 - Delta, Lãi Ròng/Tháng** *(= Tổng Thu Sau Thuế − Tiền Chủ Nhà)*:")
        st.markdown(f"<div style='background:#{'e8f5e9' if delta_lai_rong >= 0 else 'ffebee'};border-left:4px solid {delta_color};padding:8px 14px;border-radius:6px;font-size:15px;margin-bottom:8px;'>"
                    f"{delta_icon} <b style='color:{delta_color};'>{delta_lai_rong:,.0f}</b> VNĐ/tháng</div>", unsafe_allow_html=True)

        # Ô 14: Thời gian hoàn vốn = R4 = Tổng CP Bỏ Ra / Lãi Ròng/Tháng
        thoi_gian_hoan_von = None  # Khởi tạo mặc định
        st.markdown("**Ô 14 - ⏱️ Thời Gian Hoàn Vốn (Tháng)** *(= Tổng CP Bỏ Ra / Lãi Ròng/Tháng)*:")
        if delta_lai_rong > 0:
            thoi_gian_hoan_von = tong_cp_bo_ra / delta_lai_rong
            nam = int(thoi_gian_hoan_von // 12)
            thang_du = thoi_gian_hoan_von % 12
            st.markdown(f"""
            <div style='background:linear-gradient(135deg,#fff9c4,#fffde7);border:2px solid #f9a825;
                        border-radius:10px;padding:16px 20px;text-align:center;margin:10px 0;'>
                <div style='font-size:32px;font-weight:900;color:#e65100;'>{thoi_gian_hoan_von:.1f} tháng</div>
                <div style='font-size:16px;color:#555;margin-top:6px;'>≈ {nam} năm {thang_du:.1f} tháng</div>
                <div style='font-size:13px;color:#888;margin-top:4px;'>Tổng CP: {tong_cp_bo_ra:,.0f} VNĐ  ÷  Lãi ròng: {delta_lai_rong:,.0f} VNĐ/tháng</div>
            </div>
            """, unsafe_allow_html=True)
        elif delta_lai_rong == 0:
            st.error("⚠️ Lãi ròng = 0 → Không thể tính thời gian hoàn vốn!")
        else:
            st.error(f"❌ Lãi ròng âm ({delta_lai_rong:,.0f} VNĐ/tháng) → Dự án **KHÔNG có khả năng hoàn vốn** với thông số này!")

        # Bảng tổng kết
        st.markdown("---")
        st.markdown("#### 📋 Bảng Tổng Kết Các Thông Số")
        summary_data = {
            "Chỉ tiêu": [
                "Tổng Chi Phí Sau VAT",
                "Chi Phí Lãi Vay (12%/năm × 24 tháng)",
                "Tổng CP Thực Bỏ Ra (Có Lãi Vay)",
                f"Feedback Vina ({thang_fb_vina} tháng)",
                f"Feedback Mobi ({thang_fb_mobi} tháng)",
                "Tổng Chi Phí Bỏ Ra",
                "---",
                "Tiền Chủ Nhà/Tháng",
                "Viettel Trả/Tháng",
                "Vina Trả/Tháng",
                "Mobi Trả/Tháng",
                "Tổng Thu/Tháng (Gồm VAT)",
                "Tổng Thu/Tháng (Sau Thuế 10%)",
                "Delta - Lãi Ròng/Tháng",
                "⏱️ Thời Gian Hoàn Vốn"
            ],
            "Giá Trị": [
                f"{tcp_val:,.0f} VNĐ",
                f"{lai_vay_f:,.0f} VNĐ",
                f"{tong_cp_thuc:,.0f} VNĐ",
                f"{fb_vina_val:,.0f} VNĐ",
                f"{fb_mobi_val:,.0f} VNĐ",
                f"{tong_cp_bo_ra:,.0f} VNĐ",
                "───────────",
                f"{rent_t9:,.0f} VNĐ",
                f"{viettel_monthly_t9:,.0f} VNĐ",
                f"{vina_monthly_t9:,.0f} VNĐ",
                f"{mobi_monthly_t9:,.0f} VNĐ",
                f"{tong_thu_incl_vat:,.0f} VNĐ",
                f"{tong_thu_after_tax:,.0f} VNĐ",
                f"{delta_lai_rong:,.0f} VNĐ/tháng",
                f"{thoi_gian_hoan_von:.1f} tháng" if delta_lai_rong > 0 else "N/A"
            ]
        }
        df_summary_t9 = pd.DataFrame(summary_data)
        st.markdown("""
        <style>
        .t9-summary-table { width:100%;border-collapse:collapse;font-family:"Source Sans Pro",sans-serif; }
        .t9-summary-table th { background:#ffeaea!important;color:#ff0000!important;font-weight:900!important;border:1px solid #e0e0e0;padding:10px;font-size:15px; }
        .t9-summary-table td { border:1px solid #e0e0e0;padding:8px 12px;font-size:14px; }
        .t9-summary-table tr:nth-child(even) { background:#f9f9f9; }
        .t9-summary-table tr:hover { background:#fff3f3; }
        .t9-summary-table tr:last-child td { background:#fff9c4!important;font-weight:900!important;font-size:16px!important;color:#e65100!important; }
        </style>
        """, unsafe_allow_html=True)
        st.markdown(df_summary_t9.to_html(index=False, classes="t9-summary-table", escape=False), unsafe_allow_html=True)

else:
    st.info("💡 Hệ thống đang chờ liên kết Cơ Sở Dữ Liệu. File `data.xlsx` sẽ tự động kết nối khi nhìn thấy.")
