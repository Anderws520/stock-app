# ===== Python Streamlit 最終完美相容版 (精準對齊你的欄位) =====
import streamlit as st
import pandas as pd
import urllib.parse

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# ==================== [ 關鍵設定區 ] ====================
# 🟢 已為您鎖定您的試算表 ID
SPREADSHEET_ID = "1GjcN6DSFWwJG14bPyMW8aNUkE70Auz6BQFPJ9EGzR38" 
SHEET_NAME = "stock_Sheet"
# =======================================================

# 安全處理工作表名稱網址編碼
encoded_sheet_name = urllib.parse.quote(SHEET_NAME)
csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"

@st.cache_data(ttl=30)  # 測試期間快取縮短至 30 秒，方便即時看結果
def load_data(url):
    try:
        # 讀取原始 CSV
        raw_df = pd.read_csv(url, dtype=str)
        return raw_df
    except Exception as e:
        st.error(f"❌ 從 Google Sheets 讀取資料失敗: {e}")
        return None

# 讀取資料
df = load_data(csv_url)

if df is None or df.empty:
    st.warning("⚠️ 無法讀取資料，請確認試算表已開啟「知道連結的任何人都能檢視」權限。")
else:
    # 🔴 核心校正 1：清理欄位名稱的空白
    df.columns = [str(c).strip() for c in df.columns]
    
    # 🔴 核心校正 2：清除第 2 列重複表頭造成的髒資料 (過濾掉把 "日期" 當作資料內容的列)
    if "日期" in df.columns:
        df = df[df["日期"] != "日期"]
        df = df[df["日期"].str.contains("/", na=False)]  # 確保日期格式必須包含斜線
        
    # 🔴 核心校正 3：根據你圖中的實際欄位名稱進行對齊
    # 檢查你的試算表實際使用的欄位名稱
    col_mapping = {
        "買超張數": "買超張數",
        "法人買超(張)": "買超張數",
        "5日均價": "5日均價",
        "5日均價(MA5)": "5日均價",
        "超盤建議": "超盤建議"
    }
    
    # 動態調整程式內要使用的欄位代碼
    target_buy_col = "買超張數" if "買超張數" in df.columns else ("法人買超(張)" if "法人買超(張)" in df.columns else None)
    target_ma5_col = "5日均價" if "5日均價" in df.columns else ("5日均價(MA5)" if "5日均價(MA5)" in df.columns else None)
    
    if not target_buy_col or "股票代號" not in df.columns:
        st.error("❌ 試算表欄位結構不符！")
        st.info(f"目前偵測到的試算表欄位有：{list(df.columns)}")
    else:
        try:
            # 數據型態安全轉換
            df["買超_n"] = pd.to_numeric(df[target_buy_col], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            # 取得最新交易日 (排序後的第一筆日期)
            df = df.sort_values(by="日期", ascending=False)
            latest_date = df["日期"].iloc[0]
            today_df = df[df["日期"] == latest_date].copy()
            
            # 渲染前端
            st.markdown(f"### 📅 當前監控交易日：{latest_date}")
            
            # 精選 Top 3 (按買超張數排序取前三)
            top3 = today_df.sort_values(by="買超_n", ascending=False).head(3)
            
            st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
            if not top3.empty:
                # 動態動用存在的欄位，避免 KeyError
                show_cols = ["股票代號", "股票名稱", target_buy_col]
                if target_ma5_col in today_df.columns: show_cols.append(target_ma5_col)
                if "目前現價" in today_df.columns: show_cols.append("目前現價")
                if "超盤建議" in today_df.columns: show_cols.append("超盤建議")
                
                st.dataframe(top3[show_cols], use_container_width=True)
            else:
                st.info("💡 今日暫無符合標準的 Top 3 標的。")
            
            st.write("---")
            st.markdown(f"### 📋 {latest_date} 全標的監控清單")
            
            # 顯示完整表格
            full_show_cols = ["日期", "股票代號", "股票名稱", target_buy_col]
            for c in ["5日均價", "目前現價", "價差%", "出現天數", "超盤建議", "連續出現天數", "推薦等級"]:
                if c in today_df.columns:
                    full_show_cols.append(c)
                    
            st.dataframe(today_df[full_show_cols], use_container_width=True)
            
        except Exception as e:
            st.error(f"💥 運算資料時發生非預期錯誤: {e}")
