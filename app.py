import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# 🔥 1. 請在這裡換上你原本最頂、會通的公開 Google Sheets CSV 網址
# (如果這條不是你原本那條，請把它換成你原本寫在 code 裡 pd.read_csv 的那個 URL)
PUBLIC_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT1nOOnI88A8NWhd34Zco0_T2eUj-1f-BvR9kM6jGCHfIqY9n_X_0Wscv8uV_N92Rz3xsczJq1B2X0u/pub?output=csv"

try:
    # 強制不把任何行當作 header，先當作純文字二維陣列讀進來，防止被重複表頭干擾
    raw_df = pd.read_csv(PUBLIC_SHEET_URL, header=None)
except Exception as e:
    st.error(f"❌ 讀取雲端試算表失敗，請確認你的公開 CSV 網址是否正確。錯誤訊息: {e}")
    st.stop()

# --- 2. 徹底剝離重複表頭，精準對齊資料 (從第 3 列開始才是富邦、統一等黃金資料) ---
# 根據你的 image_1819cb.png 截圖，真正股票資料是從 Excel 第 3 列 (index 2) 開始
data_rows = raw_df.iloc[2:].copy()

# 根據截圖上資料實際產生的位置，我們「不認名稱，直接認位置」進行強制對齊：
clean_df = pd.DataFrame()

try:
    clean_df["日期"] = data_rows[0].astype(str).str.strip()
    clean_df["股票代號"] = data_rows[1].astype(str).str.replace("'", "").str.strip()
    clean_df["股票名稱"] = data_rows[2].astype(str).str.strip()
    clean_df["關鍵分點"] = data_rows[3].astype(str).str.strip()
    
    # 數值型態處理 (轉換失敗就變 0)
    clean_df["買超張數_n"] = pd.to_numeric(data_rows[4], errors='coerce').fillna(0)
    clean_df["5日均價"] = pd.to_numeric(data_rows[5], errors='coerce').fillna(0)
    clean_df["目前現價"] = pd.to_numeric(data_rows[6], errors='coerce').fillna(0)
    
    # 價差% 完美還原 (試算表內是 0.06 轉成 6.00%)
    clean_df["價差%_n"] = pd.to_numeric(data_rows[7], errors='coerce').fillna(0)
    clean_df["價差%"] = clean_df["價差%_n"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
    
    # 截圖後半段的精華欄位對齊
    clean_df["出現天數"] = data_rows[8].fillna("")
    clean_df["超盤建議"] = data_rows[9].fillna("")
    clean_df["連續出現天數"] = data_rows[10].fillna("")
    clean_df["集保人數變動"] = data_rows[11].fillna("")
    clean_df["最佳購買日期"] = data_rows[12].fillna("")

except Exception as e:
    st.error(f"💥 對齊試算表欄位時發生錯誤，可能雲端欄位順序有變動。錯誤: {e}")
    st.stop()

# --- 3. 篩選最新交易日與渲染畫面 ---
if clean_df.empty:
    st.warning("⚠️ 目前清理後的資料庫為空，請檢查 Google Sheets 內容。")
else:
    # 自動抓取第一筆（通常是最新的）日期
    latest_date = clean_df["日期"].iloc[0]
    
    # 篩選當日所有資料
    today_df = clean_df[clean_df["日期"] == latest_date].copy()
    
    # 重新整理方便給 Top 3 顯示的買超張數格式
    today_df["買超張數"] = today_df["買超張數_n"].apply(lambda x: f"{x:,.1f}" if x != 0 else "0")
    
    # 算出正宗的 Top 3 (依據買超張數大到小排序)
    top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3)

    st.markdown(f"### 📅 當前監控交易日：{latest_date}")
    
    # ----- TOP 3 區塊 -----
    st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
    if not top3.empty:
        st.dataframe(
            top3[["股票代號", "股票名稱", "關鍵分點", "買超張數", "目前現價", "5日均價", "價差%", "出現天數", "超盤建議"]],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("💡 今日暫無符合標準的 Top 3 標的。")
        
    st.write("---")
    
    # ----- 全標的一覽 -----
    st.markdown(f"### 📋 {latest_date} 全標的監控清單")
    st.dataframe(
        today_df[["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "出現天數", "連續出現天數", "集保人數變動", "最佳購買日期", "超盤建議"]],
        use_container_width=True,
        hide_index=True
    )
