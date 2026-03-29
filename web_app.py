import streamlit as st
import pandas as pd
import io
import os

# --- CẤU HÌNH GIAO DIỆN WEB ---
st.set_page_config(page_title="Hệ Thống Tra Cứu Hợp Đồng", page_icon="📡", layout="wide")

st.title("📡 Hệ Thống Viễn Thông - Tra Cứu Hợp Đồng Trạm")
st.markdown("Hệ thống Web thông minh, được tối ưu hoàn hảo cho cả trải nghiệm **Trên Máy Tính** lẫn **Màn Hình Di Động** (Android/iOS).")

TARGET_COLUMNS = [
    "mã trạm", "Q/H", "long thuê", "lat thuê", "Địa chỉ", 
    "Viettel", "Vina", "Mobi", 
    "Ngày ký HĐ Chủ nhà_trên HĐ", "Ngày hết hạn HĐ", 
    "Chủ nhà + SĐT", "giá thuê chủ nhà", "Giá Viettel Thuê", 
    "Giá MB thuê", "Giá Vina thuê", "chu kỳ thanh toán cho chủ nhà", 
    "Số HĐ với chủ nhà", "Số TK chủ nhà", "Chủ tài khoản", "Tên Ngân Hàng"
]

def normalize_str(s):
    return str(s).strip().lower()

# --- HÀM XỬ LÝ DỮ LIỆU & LƯU VÀO CACHE BỘ NHỚ ---
@st.cache_data
def load_data(file_source):
    try:
        sheet_name_target = "Theo dõi HĐ_Chi tiết"
        xl = pd.ExcelFile(file_source)
        
        target_sheet = next((s for s in xl.sheet_names if sheet_name_target.lower() in s.lower()), None)
        if not target_sheet:
            st.error(f"⚠️ Không tìm thấy sheet có chứa chữ '{sheet_name_target}' trong file Excel.")
            return pd.DataFrame()

        df = pd.read_excel(file_source, sheet_name=target_sheet)
        
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
        
        for col in df_filtered.columns:
            if pd.api.types.is_datetime64_any_dtype(df_filtered[col]):
                df_filtered[col] = df_filtered[col].dt.strftime('%d/%m/%Y')
        
        return df_filtered.fillna("")
    except Exception as e:
        st.error(f"⚠️ Có lỗi trong quá trình đọc Excel: {e}")
        return pd.DataFrame()

# --- LIÊN KẾT NHÚNG DỮ LIỆU CỨNG ---
df_source = pd.DataFrame()

st.sidebar.header("📁 Dữ Liệu Báo Cáo")
st.sidebar.markdown("Cơ chế tàng hình nhúng sẵn file không cần Upload thủ công.")

# Tên file Excel mặc định để đọc
DEFAULT_FILE = "data.xlsx"

if os.path.exists(DEFAULT_FILE):
    st.sidebar.success(f"✅ Đã kết nối tự động với CSDL được cấp: `{DEFAULT_FILE}`")
    df_source = load_data(DEFAULT_FILE)
else:
    st.sidebar.warning(f"⚠️ Dữ liệu gốc `{DEFAULT_FILE}` chưa được thiết lập tự động! Bạn hãy tải file lên hoặc đổi tên file Excel hiện tại thành `{DEFAULT_FILE}` và copy đè vào thư mục mã nguồn nhé.")
    
# Nhúng File Uploader như một công cụ hỗ trợ phòng hờ file cứng bị lỗi
uploaded_file = st.sidebar.file_uploader("Mở Cổng Tải File Phụ Trợ:", type=["xlsx", "xls"])
if uploaded_file is not None:
    df_source = load_data(uploaded_file)
    st.sidebar.success("✅ Nạp liệu file thành công!")

# --- GIAO DIỆN HIỂN THỊ CHÍNH ---
if not df_source.empty:
    st.markdown("### 🔍 Phễu Lọc Mã Trạm")
    input_text = st.text_area("Hộp dán mã trạm thông minh (Bạn hãy copy Paste một hay nhiều mã trạm vào đây, mỗi máy cách nhau dấu phẩy hoặc enter xuống dòng. Để trống để xem rổ dữ liệu gốc):", height=120)
    
    df_display = df_source.copy()
    
    if input_text.strip():
        target_stations = [s.strip().lower() for s in input_text.replace(',', '\n').split('\n') if s.strip()]
        if target_stations:
            mask = df_display["mã trạm"].astype(str).str.strip().str.lower().isin(target_stations)
            df_display = df_display[mask]
            
    if df_display.empty:
        st.warning("❌ Không tìm thấy mã trạm phù hợp trong Cụm Dữ Liệu này.")
    else:
        st.success(f"✅ Quét thành công! Có **{len(df_display)}** trạm khớp với mã tra cứu.")
        
        # Bảng Dataframe của Streamlit siêu mạnh, hỗ trợ vuốt chạm trên mobile
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        
        # Tiện ích bổ sung: Móc Download Data
        st.markdown("### 📥 Tải Danh Sách Tìm Thấy")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_display.to_excel(writer, index=False, sheet_name='DuLieuLọc')
        excel_data = output.getvalue()
        
        st.download_button(
            label="🔽 Tải Về Trực Tiếp Điện Thoại Báo Cáo Rút Gọn Này",
            data=excel_data,
            file_name="Ket_Qua_Tram.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
else:
    st.info("💡 Hệ thống trống Dữ Liệu Cơ Sở. Không thể hiển thị thống kê.")

