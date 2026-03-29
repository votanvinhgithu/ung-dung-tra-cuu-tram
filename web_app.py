import streamlit as st
import pandas as pd
import io
import os
from datetime import datetime

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
    
    # Fuzzy match 'mã trạm' and 'số tiền thanh toán'
    for c in df_pay.columns:
        c_low = str(c).strip().lower()
        if "mã trạm" in c_low or "mã" in c_low:
            if ma_tram_col is None: ma_tram_col = c
        if "số tiền thanh toán" in c_low or "số tiền" in c_low:
            if amount_col is None: amount_col = c
            
    if not ma_tram_col: return df_main # Bỏ qua nếu sheet 2 không có cột mã trạm
    
    date_cols = [c for c in df_pay.columns if c != ma_tram_col and c != amount_col]
    
    for _, row in df_pay.iterrows():
        ma_tram = str(row[ma_tram_col]).strip().lower() if pd.notna(row[ma_tram_col]) else ""
        if not ma_tram: continue
        
        # Bốc KHỚP nguyên giá trị của Cột (không tính theo tháng, lấy chính xác số tổng trên Excel)
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
        # Format the amount beautifully, e.g., 10,000,000
        formatted_amount = f"{amount_val:,.0f}" if amount_val > 0 else "-"
        
        pay_dict[ma_tram] = {
            "Ngày tới hạn TT trong tháng": fmt(due_date) if due_date else "Không có",
            "Ngày TT kỳ trước": fmt(prev_date),
            "Ngày đến hạn TT kỳ tiếp theo": fmt(next_date),
            "Số tiền cần thanh toán": formatted_amount,
            "__raw_amount__": amount_val,
            "__is_due_this_month__": bool(due_date)
        }
        
    df_res = df_main.copy()
    new_cols = {
        "Ngày tới hạn TT trong tháng": [],
        "Ngày TT kỳ trước": [],
        "Ngày đến hạn TT kỳ tiếp theo": [],
        "Số tiền cần thanh toán": [],
        "__raw_amount__": [],
        "__is_due_this_month__": []
    }
    
    import re
    for _, row in df_res.iterrows():
        ma = str(row.get("mã trạm", "")).strip().lower()
        info = pay_dict.get(ma, {})
        
        # Tiền gốc từ cột "số tiền thanh toán" ở Sheet 2 (đang là tiền thuể 1 tháng)
        base_monthly = info.get("__raw_amount__", 0.0)
        
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
        
        # TRỌNG TÂM: SỐ TIỀN THANH TOÁN = Tiền 1 tháng * Số tháng chu kỳ
        calc_amount = base_monthly * real_cycle
        
        if calc_amount > 0:
            formatted_amount = f"{calc_amount:,.0f}"
        else:
            formatted_amount = "-"
            
        new_cols["Ngày tới hạn TT trong tháng"].append(info.get("Ngày tới hạn TT trong tháng", "-"))
        new_cols["Ngày TT kỳ trước"].append(info.get("Ngày TT kỳ trước", "-"))
        new_cols["Ngày đến hạn TT kỳ tiếp theo"].append(info.get("Ngày đến hạn TT kỳ tiếp theo", "-"))
        
        # Ghi đè vào kết quả hiển thị
        new_cols["Số tiền cần thanh toán"].append(formatted_amount)
        new_cols["__raw_amount__"].append(calc_amount)
        new_cols["__is_due_this_month__"].append(info.get("__is_due_this_month__", False))
        
    for k, v in new_cols.items():
        df_res[k] = v
        
    return df_res

# --- HÀM XỬ LÝ DỮ LIỆU & LƯU VÀO CACHE BỘ NHỚ ---
@st.cache_data
def load_data_and_enrich_v2(file_source, target_month_str):
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
month_input = st.sidebar.text_input("📅 Tùy chỉnh Tháng Tra Cứu (MM/YYYY):", value=current_mm_yyyy)

# Hỗ trợ tự động nhận diện cả chữ hoa chữ thường
DEFAULT_FILE = ""
if os.path.exists("data.xlsx"):
    DEFAULT_FILE = "data.xlsx"
elif os.path.exists("Data.xlsx"):
    DEFAULT_FILE = "Data.xlsx"

df_source = pd.DataFrame()

if DEFAULT_FILE:
    st.sidebar.success(f"✅ Đã kết nối tự động với CSDL gốc: `{DEFAULT_FILE}`")
    df_source = load_data_and_enrich_v2(DEFAULT_FILE, month_input)
else:
    st.sidebar.warning(f"⚠️ Vùng Nhúng Ngầm Trống! Bạn hãy File Excel vào GitHub nhé.")
    uploaded_file = st.sidebar.file_uploader("Hoặc tải file Excel tạm thời lên đây:", type=["xlsx", "xls"])
    if uploaded_file is not None:
        df_source = load_data_and_enrich_v2(uploaded_file, month_input)

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
    tab1, tab2 = st.tabs(["🔍 LỌC MÃ TRẠM THEO YÊU CẦU", "💵 DS TRẠM CẦN THANH TOÁN (SHEET 2)"])

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
                
                st.markdown("### 🏷️ Chi Tiết Dạng Thẻ (Dành cho Điện thoại)")
                render_cards(df_display, is_payment_tab=False)
                
                st.markdown("---")
                st.markdown("### 📊 Tổng Hợp Lưới Ngang (Xem trọn bộ Hàng Ngang)")
                df_clean_tab1 = df_display.drop(["__raw_amount__", "__is_due_this_month__"], axis=1, errors='ignore')
                st.dataframe(df_clean_tab1, use_container_width=True, hide_index=True)
                
    # ------------ TAB 2: QUẢN LÝ TỔNG THANH TOÁN THÁNG ------------
    with tab2:
        st.markdown(f"### 💵 Các trạm Cần Thanh Toán trong **{month_input}**")
        with st.form(key='payment_form'):
            st.info("Bấm lọc để thống kê Số Tiền tổng của tháng. Có thể nhập mã CỤ THỂ 1 TRẠM phía dưới để rà soát.")
            search_tab2 = st.text_input("🔍 Tra cứu cụ thể một (hoặc nhiều) mã trạm trong DS của tháng này (Để trống là Tính Tổng Tất Cả):", placeholder="Ví dụ: HCM001, HCM002...")
            submit_payment_filter = st.form_submit_button(label="🔍 LỌC CÁC TRẠM TỚI HẠN THANH TOÁN THÁNG NÀY", use_container_width=True)
            
        if submit_payment_filter:
            df_pay_display = df_source[df_source["__is_due_this_month__"] == True].copy()
            
            # Lọc theo trạm cụ thể nếu người dùng có gõ
            if search_tab2.strip():
                target_stations_2 = [s.strip().lower() for s in search_tab2.replace(',', '\n').split('\n') if s.strip()]
                if target_stations_2:
                    mask2 = df_pay_display["mã trạm"].astype(str).str.strip().str.lower().isin(target_stations_2)
                    df_pay_display = df_pay_display[mask2]
            
            if df_pay_display.empty:
                st.warning(f"❌ Các mã trạm đó không cần thanh toán trong tháng {month_input} này.")
            else:
                total_stations = len(df_pay_display)
                total_amount = df_pay_display["__raw_amount__"].sum()
                
                # Cảnh báo rực rỡ báo cáo ngân sách
                st.snow()
                st.success(f"🔥 **TỔNG KẾT BÁO CÁO NHANH THÁNG {month_input}:**")
                colA, colB = st.columns(2)
                colA.metric("🏢 Tổng số trạm hiển thị:", f"{total_stations} trạm")
                colB.metric("💰 Tổng tiền giải ngân:", f"{total_amount:,.0f} VNĐ")
                
                st.markdown("---")
                st.markdown("### 🏷️ Chi Tiết Các Trạm Trong Hạng Mục")
                render_cards(df_pay_display, is_payment_tab=True)
                
                st.markdown("---")
                st.markdown("### 📊 Tổng Hợp Lưới Ngang (Xem trọn bộ Hàng Ngang)")
                df_clean_tab2 = df_pay_display.drop(["__raw_amount__", "__is_due_this_month__"], axis=1, errors='ignore')
                st.dataframe(df_clean_tab2, use_container_width=True, hide_index=True)
                
                # Nút tải xuống cho báo cáo Tab 2
                output2 = io.BytesIO()
                with pd.ExcelWriter(output2, engine='openpyxl') as writer:
                    df_clean_tab2.to_excel(writer, index=False, sheet_name='TraCuuThang')
                excel_data2 = output2.getvalue()
                st.download_button(
                    label="🔽 NHẤN TẢI XUỐNG BÁO CÁO THANH TOÁN 1 THÁNG NÀY (EXCEL)",
                    data=excel_data2,
                    file_name=f"Bao_Cao_Thanh_Toan_{month_input.replace('/','_')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
else:
    st.info("💡 Hệ thống đang chờ liên kết Cơ Sở Dữ Liệu. File `data.xlsx` sẽ tự động hiển thị ra khi quét thấy.")
