import streamlit as st
import pandas as pd
import io
import os
from datetime import datetime
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
                
                # Xử lý Rút Giá trị Trả/Tháng (Hỗ trợ tốt dạng số có chấm 5.000.000 của VN)
                raw_monthly = str(row[monthly_col])
                digits_m = re.sub(r'\D', '', raw_monthly)
                monthly_val = float(digits_m) if digits_m else 0.0
                    
                # Lấy Mặc Định Giá trị từ Cột CHÓT CÙNG (Số Tiền Thanh Toán Doanh Thu)
                raw_payment = str(row[last_col_idx])
                digits_p = re.sub(r'\D', '', raw_payment)
                payment_val = float(digits_p) if digits_p else 0.0
                    
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

# --- HÀM XỬ LÝ DỮ LIỆU & LƯU VÀO CACHE BỘ NHỚ ---
# V3 để phá vỡ Cache cũ của website trên mạng
@st.cache_data
def load_data_and_enrich_v3(file_source, target_month_str):
    try:
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
    tab1, tab2, tab3 = st.tabs(["🔍 LỌC THÔNG TIN MÃ TRẠM", "💵 DS TRẠM TT CHỦ NHÀ", "💰 DOANH THU CÁC NHÀ MẠNG"])

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
                
                st.markdown("### 📊 Tổng Hợp Lưới Ngang (Xem trọn bộ Hàng Ngang)")
                df_clean_tab1 = df_display.drop(["__raw_amount__", "__is_due_this_month__"], axis=1, errors='ignore')
                # Chèn thêm cột Số thứ tự ở vị trí đầu tiên
                df_clean_tab1.insert(0, 'STT', range(1, len(df_clean_tab1) + 1))
                st.dataframe(df_clean_tab1, use_container_width=True, hide_index=True)
                
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
            
            date_limit_tab2 = st.text_input("⏳ Lọc CỤM Trạm Cần TT Ghi sổ Tính Tới Mốc Ngày (Bỏ qua nếu muốn xem Cả Tháng):", placeholder="Gõ cực chuẩn định dạng MM/DD/YYYY. Ví dụ: 03/15/2026")
            
            submit_payment_filter = st.form_submit_button(label="🔍 TRA CỨU DANH SÁCH", use_container_width=True)
            
        if submit_payment_filter:
            # Nhồi lại Data Engine đặc biệt theo Option Tháng vừa nhập!
            df_pay_source = load_data_and_enrich_v3(DEFAULT_FILE, month_input_tab2) if DEFAULT_FILE else df_source
            df_pay_display = df_pay_source[df_pay_source["__is_due_this_month__"] == True].copy()
            
            # Lọc theo trạm cụ thể nếu người dùng có gõ
            if search_tab2.strip():
                target_stations_2 = [s.strip().lower() for s in search_tab2.replace(',', '\n').split('\n') if s.strip()]
                if target_stations_2:
                    mask2 = df_pay_display["mã trạm"].astype(str).str.strip().str.lower().isin(target_stations_2)
                    df_pay_display = df_pay_display[mask2]
                    
            # Lọc đếm ngược Tới Mốc Ngày (Phục vụ Chốt Quỹ Giải Ngân Kế Toán)
            if date_limit_tab2.strip():
                try:
                    limit_dt = pd.to_datetime(date_limit_tab2.strip(), format='%m/%d/%Y')
                    temp_dt = pd.to_datetime(df_pay_display['Ngày tới hạn TT trong tháng'], format='%m/%d/%Y', errors='coerce')
                    mask_date = (temp_dt <= limit_dt) & (temp_dt.notna())
                    df_pay_display = df_pay_display[mask_date]
                except Exception:
                    st.warning("⚠️ Lỗi định dạng ngày chốt sổ! Bạn phải gõ dấy sẹc '/' theo mẫu chuẩn tháng MM/DD/YYYY (Ví dụ: 03/15/2026)")
            
            if df_pay_display.empty:
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
                if date_limit_tab2.strip():
                    st.success(f"🔥 **BÁO CÁO GIẢI NGÂN GẤP (CHỈ TÍNH CÁC HĐ ĐẾN CHỐT NGÀY {date_limit_tab2.strip()} CỦA THÁNG {month_input_tab2}):**")
                else:
                    st.success(f"🔥 **TỔNG KẾT BÁO CÁO NHANH LŨY KẾ CẢ THÁNG NAY ({month_input_tab2}):**")
                    
                colA, colB = st.columns(2)
                colA.metric("🏢 Tổng số trạm hiển thị:", f"{total_stations} trạm")
                colB.metric("💰 Tổng tiền giải ngân:", f"{total_amount:,.0f} VNĐ")
                
                st.markdown("---")
                st.markdown("### 📊 Tổng Hợp Lưới Ngang (Báo cáo Lọc Dạng Bảng Excel)")
                df_clean_tab2 = df_pay_display.drop(["__raw_amount__", "__is_due_this_month__"], axis=1, errors='ignore')
                # Chèn thêm cột Số thứ tự ở vị trí đầu tiên
                df_clean_tab2.insert(0, 'STT', range(1, len(df_clean_tab2) + 1))
                st.dataframe(df_clean_tab2, use_container_width=True, hide_index=True)

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

else:
    st.info("💡 Hệ thống đang chờ liên kết Cơ Sở Dữ Liệu. File `data.xlsx` sẽ tự động kết nối khi nhìn thấy.")
