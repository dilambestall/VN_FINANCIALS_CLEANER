import pandas as pd
import os

# ==== Hàm tiện ích ====
def get_value(df, keywords, year_col):
    if isinstance(keywords, str):
        keywords = [keywords]

    for kw in keywords:
        row = df[df.iloc[:, 0].astype(str).str.contains(kw, case=False, na=False)]
        if not row.empty:
            val = row.iloc[0][year_col]
            val = str(val).replace(",", "").replace(" ", "")
            try:
                return float(val)
            except:
                return None
    return None


def read_with_auto_header(file_path, sheet_name):
    """Tự động tìm dòng header (có chứa 'Năm/20xx')."""
    tmp = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=15)
    header_row = None
    for i, row in tmp.iterrows():
        if row.astype(str).str.contains("Năm/20", case=False, na=False).any():
            header_row = i
            break

    if header_row is None:
        raise ValueError(f"Không tìm thấy dòng header trong sheet {sheet_name}")

    return pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)


def process_company_multi_year(company_id, file_path):
    """Đọc 1 file Excel và trích xuất dữ liệu nhiều năm."""
    cdkt = read_with_auto_header(file_path, "CÂN ĐỐI KẾ TOÁN")
    kqkd = read_with_auto_header(file_path, "KẾT QUẢ KINH DOANH")
    lctt = read_with_auto_header(file_path, "LƯU CHUYỂN TIỀN TỆ")

    results = []
    # tìm các năm trong header
    years = []
    for col in cdkt.columns:
        if "Năm/" in str(col):
            try:
                years.append(int(str(col).split("/")[-1]))
            except:
                pass

    for year in years:
        year_col = None
        for col in cdkt.columns:
            if str(year) in str(col):
                year_col = col
                break

        if year_col is None:
            continue

        features = {
            "company_id": company_id,
            "year": year,

            # Balance Sheet
            "total_assets": get_value(cdkt, "Tổng cộng tài sản", year_col),
            "equity": get_value(cdkt, "Vốn chủ sở hữu", year_col),
            "total_liabilities": get_value(cdkt, "Nợ phải trả", year_col),
            "current_assets": get_value(cdkt, "Tài sản ngắn hạn", year_col),
            "current_liabilities": get_value(cdkt, "Nợ ngắn hạn", year_col),
            "cash_and_equivalents": get_value(cdkt, "Tiền và các khoản tương đương tiền", year_col),
            "short_term_debt": get_value(cdkt, "Vay và nợ thuê tài chính ngắn hạn", year_col),
            "long_term_debt": get_value(cdkt, "Vay và nợ thuê tài chính dài hạn", year_col),

            # Income Statement
            "revenue": get_value(kqkd, ["Doanh thu bán hàng", "Doanh thu thuần"], year_col),
            "gross_profit": get_value(kqkd, "Lợi nhuận gộp", year_col),
            "net_income": get_value(kqkd, ["Lợi nhuận sau thuế", "Lợi nhuận sau thuế thu nhập DN"], year_col),
            "selling_expenses": get_value(kqkd, "Chi phí bán hàng", year_col),
            "admin_expenses": get_value(kqkd, "Chi phí quản lý doanh nghiệp", year_col),
            "interest_expenses": get_value(kqkd, "Chi phí tài chính", year_col),

            # Cash Flow
            "cashflow_ops": get_value(lctt, [
                "Lưu chuyển tiền thuần từ hoạt động kinh doanh",
                "I. Lưu chuyển tiền từ hoạt động kinh doanh"
            ], year_col),
            "cashflow_investing": get_value(lctt, [
                "Lưu chuyển tiền thuần từ hoạt động đầu tư",
                "II. Lưu chuyển tiền từ hoạt động đầu tư"
            ], year_col),
            "cashflow_financing": get_value(lctt, [
                "Lưu chuyển tiền thuần từ hoạt động tài chính",
                "III. Lưu chuyển tiền từ hoạt động tài chính"
            ], year_col),
        }
        results.append(features)

    return pd.DataFrame(results)


# ==== Chạy cho tất cả công ty ====
def process_all_companies(base_dir="data/landing", output_path="data/cleaned/all_companies.csv"):
    all_data = []
    for company_id in os.listdir(base_dir):
        company_dir = os.path.join(base_dir, company_id)
        if not os.path.isdir(company_dir):
            continue

        # Lấy tất cả file .xlsx trong thư mục công ty
        for file in os.listdir(company_dir):
            if file.endswith(".xlsx"):
                file_path = os.path.join(company_dir, file)
                print(f"📂 Đang xử lý {company_id}: {file}")
                try:
                    df = process_company_multi_year(company_id, file_path)
                    all_data.append(df)
                except Exception as e:
                    print(f"⚠️ Lỗi {company_id}: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ Done! Đã lưu dữ liệu tại {output_path}")
        print(final_df.head())
    else:
        print("⚠️ Không có dữ liệu nào được xử lý.")


if __name__ == "__main__":
    process_all_companies()
