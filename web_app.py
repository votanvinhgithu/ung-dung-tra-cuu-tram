import streamlit as st
import pandas as pd
import io
import os

# --- CẤU HÌNH GIAO DIỆN WEB ---
st.set_page_config(page_title="Hệ Thống Tra Cứu Hợp Đồng", page_icon="📡", layout="wide")

st.title("📡 Hệ Thống Viễn Thông - Tra Cứu Hợp Đồng Trạm")
st.markdown("Giao diện siêu hiện đại được làm lại thân thiện tuyệt đối với màn hình **Điện Thoại Di Động**.")

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

DEFAULT_FILE = "data.xlsx"

if os.path.exists(DEFAULT_FILE):
    st.sidebar.success(f"✅ Đã kết nối tự động với CSDL được cấp: `{DEFAULT_FILE}`")
    df_source = load_data(DEFAULT_FILE)
else:
    st.sidebar.warning(f"⚠️ Dữ liệu gốc `{DEFAULT_FILE}` chưa được thiết lập tự động! Vui lòng đẩy file `data.xlsx` lên GitHub.")
    
uploaded_file = st.sidebar.file_uploader("Mở Cổng Tải File Phụ Trợ:", type=["xlsx", "xls"])
if uploaded_file is not None:
    df_source = load_data(uploaded_file)
    st.sidebar.success("✅ Nạp liệu file thành công!")

# --- GIAO DIỆN HIỂN THỊ CHÍNH (TỐI ƯU MOBILE) ---
if not df_source.empty:
    st.markdown("---")
    
    # SỬ DỤNG FORM ĐỂ gom nhóm hành động gõ và tạo nút Bấm Kính Lúp
    with st.form(key='search_form'):
        st.markdown("### 🔍 Nhập Mã Trạm Cần Tra Cứu")
        input_text = st.text_area("Hộp dán mã trạm thông minh (Dán nhiều mã ngăn cách bằng phẩy hoặc cứ enter xuống dòng là dính. Nếu để trống sẽ hiển thị tất cả):", height=100)
        
        # Nút nhấn mới được gắn Biểu tượng kính lúp to đẹp
        submit_button = st.form_submit_button(label="🔍 TÌM KIẾM TRẠM MATCH", use_container_width=True)
    
    # Chỉ khi nào bấm Nút Kính lúp thì Code xử lý ở dưới mới chạy
    if submit_button:
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
            
            # --- HIỂN THỊ THÔNG TIN THEO CHIỀU DỌC (Dành riêng cho Màn Hình Điện Thoại) ---
            st.markdown("### 🏷️ Chi Tiết Hợp Đồng (Vuốt Dọc Giống App Điện Thoại)")
            
            # Kỹ thuật chia thẻ dọc: Chống treo máy nếu dán cả ngàn trạm
            if len(df_display) > 50:
                st.warning(f"⚠️ Do kết quả trả về quá lớn ({len(df_display)} trạm), ứng dụng sẽ chỉ hiển thị Khung Thẻ Dọc cho 50 trạm đầu tiên để tránh đứng màn hình điện thoại. Anh/chị có thể xem Bảng dữ liệu thô bị gộp bên dưới nhe.")
                display_cards = df_display.head(50)
            else:
                display_cards = df_display
                
            for index, row in display_cards.iterrows():
                tram_id = str(row["mã trạm"]) if pd.notna(row["mã trạm"]) and str(row["mã trạm"]) != "" else "Không Mẫu"
                
                # Tạo một thẻ Expand có thể tự đóng/mở được, tiêu đề Cực Rõ Nét
                with st.expander(f"📌 Bấm vào để xem Trạm: {tram_id}", expanded=True):
                    # Duyệt và in tất cả các cột theo dòng văn bản (Thay vì hiển thị bảng ngang)
                    for col in TARGET_COLUMNS:
                        val = row[col]
                        if pd.isna(val) or str(val).strip() == "":
                            val = "-"  # Thay ô trống bằng nét gạch
                        st.markdown(f"**{col}:** &nbsp;&nbsp; {val}")

            # Lưu lại Bảng siêu bự vào một Tab để nhỡ ai xài Desktop vẫn quẩy được
            with st.expander("📊 Bấm Thêm Vào Đây Để Bật Lại Bảng Báo Cáo Dạng Lưới Ngang (Excel)", expanded=False):
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            
            # Tiện ích bổ sung: Móc Download Data
            st.markdown("---")
            st.markdown("### 📥 Lưu Toàn Bộ Danh Sách Về Điện Thoại")
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_display.to_excel(writer, index=False, sheet_name='DuLieuLọc')
            excel_data = output.getvalue()
            
            st.download_button(
                label="🔽 Nhấn Tại Đây Để Tải File Excel Xuống Máy",
                data=excel_data,
                file_name="Ket_Qua_Tram_Mobile.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )
else:
    st.info("💡 Hệ thống đang chờ liên kết Cơ Sở Dữ Liệu. File `data.xlsx` sẽ tự động hiển thị ra khi quét thấy.")
