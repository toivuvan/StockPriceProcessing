# file: crawl_all_stock.py
from vnstock import Vnstock, Listing
import pandas as pd
import os
from datetime import date
import time  # <-- THÊM VÀO 1: Import thư viện time

def crawl_all_data(save_dir='data', start_date='2010-01-01'):
    # Tạo thư mục lưu dữ liệu
    os.makedirs(save_dir, exist_ok=True)

    # Khởi tạo đối tượng Vnstock (dùng để lấy giá)
    vns = Vnstock()

    # Lấy danh sách tất cả công ty niêm yết
    print("Đang lấy danh sách công ty niêm yết...")
    try:
        listing = Listing() 
        companies = listing.all_symbols()
    except Exception as e:
        print(f"Lỗi khi lấy danh sách công ty: {e}")
        return

    if companies.empty:
        print("Không lấy được danh sách công ty.")
        return
    
    # 1. Xác định tên cột ticker
    if 'ticker' in companies.columns:
        ticker_col = 'ticker'
    elif 'symbol' in companies.columns:
        ticker_col = 'symbol'
    else:
        print("Lỗi: Không tìm thấy cột 'ticker' hoặc 'symbol' trong danh sách công ty.")
        return

    # 2. Sắp xếp DataFrame theo cột ticker (alphabet)
    print(f"Đang sắp xếp {len(companies)} công ty theo cột '{ticker_col}'...")
    companies = companies.sort_values(by=ticker_col, ignore_index=True)
    
    # Lưu thông tin công ty (đã được sắp xếp)
    info_path = os.path.join(save_dir, 'companies_info.csv')
    companies.to_csv(info_path, index=False)
    print(f"Đã lưu thông tin {len(companies)} công ty vào {info_path}")

    # Ngày kết thúc là hôm nay
    end_date = date.today().strftime('%Y-%m-%d')

    # Lặp qua từng công ty để lấy dữ liệu giá
    for i, row in companies.iterrows():
        ticker = row[ticker_col]
        
        # In trạng thái (thêm số thứ tự)
        print(f"[{i+1}/{len(companies)}] Đang lấy dữ liệu: {ticker}")

        try:
            stock = vns.stock(symbol=ticker, source='VCI')
            df = stock.quote.history(start=start_date, end=end_date)

            if df is not None and not df.empty:
                file_path = os.path.join(save_dir, f"{ticker}.csv")
                df.to_csv(file_path, index=False)
                print(f"Lưu thành công {ticker}.csv ({len(df)} dòng)")
            else:
                print(f"Không có dữ liệu cho {ticker}")

        except Exception as e:
            error_msg = str(e)
            print(f"Lỗi khi lấy dữ liệu {ticker}: {error_msg}")
            
            # Xử lý lỗi Rate Limit một cách thông minh:
            if "Rate limit" in error_msg or "quá nhiều request" in error_msg:
                print("Phát hiện Rate Limit. Tạm nghỉ 60 giây...")
                time.sleep(60) # Nghỉ 1 phút rồi thử lại
                print("Tiếp tục crawl...")

        # <-- THÊM VÀO 2: Nghỉ 0.5 giây sau MỖI lần gọi
        # Giảm con số này (vd: 0.2) nếu muốn nhanh hơn, 
        # tăng (vd: 1) nếu vẫn bị lỗi.
        time.sleep(0.5) 

    print("Hoàn tất crawl tất cả dữ liệu!")

if __name__ == "__main__":
    crawl_all_data()