# ===== Python Streamlit 回歸初心・絕不報錯完美版 =====
import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# --- 1. 帳號直連（完全不 import 額外套件，防呆開到最大） ---
try:
    # 直接使用 Streamlit 內建的 gsheets 連線，吃你原本就綁好的後台帳號
    conn = st.connection("gsheets")
    # 直接讀取分頁，預設第一行就是最完美的標頭
    df = conn.read(worksheet="stock_Sheet")
except Exception as e:
    st.error(f"❌ 透過 Google 帳號讀取試算表失敗，請檢查後台連線設定。錯誤訊息: {e}")
    st.stop()

# --- 2. 欄位與資料極致清洗 ---
if df is None or df.empty:
    st.warning("⚠️ 讀取到的分頁目前沒有資料，請確認 stock_Sheet 內有內容。")
else:
    # 清理欄位名稱（去除任何隱形空白）
    df.columns = [str(c).strip() for c in df.columns]
    
    # 嚴格對齊你 image_1819cb.png 截圖上的 14 個標準欄位
    required_cols = [
        "日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", 
        "5日均價", "目前現價", "價差%", "出現天數", "超盤建議", 
        "連續出現天數", "集保人數變動", "最佳購買日期"
    ]
    
    # 檢查是否有欄位遺漏（若試算表後段有些是空欄位，先自動補齊防呆）
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    try:
        # 3. 資料型態安全轉換，防止排序或格式化崩潰
        df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
        df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
        
        # 4. 價差% 完美轉換：將試算表原始值（例如 0.06）漂亮地轉成 6.00% 展示
        df["價差%_n"] = pd.to_numeric(df["價差%"], errors='coerce').fillna(0)
        df["價差%_display"] = df["價差%_n"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
        
        # 5. 確保日期格式乾淨，並自動抓取最新交易日的資料
        df["日期"] = df["日期"].astype(str).str.strip()
        latest_date = df["日期"].iloc[0] if not df.empty else "未知的日期"
        
        # 篩選出最新一天的所有股票資料
        today_df = df[df["日期"] == latest_date].copy()
        today_df["價差%"] = today_df["價差%_display"]  # 把百分比丟回畫面顯示
        
        # 6. 精選 Top 3 邏輯（按買超張數從大到小排序，取前 3 名）
        top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3)
        
        # --- 7. 渲染大器、乾淨的看盤網頁畫面 ---
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
        
    except Exception as e:
        st.error(f"💥 運算資料時發生非預期錯誤: {e}")
