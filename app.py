import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# =======================================================
# 🟢 唯一要改的地方：請把下面這行引號內的網址，換成你最原本確定能動的網址
# =======================================================
DATA_URL = "https://docs.google.com/spreadsheets/d/e/xxxx/pub?output=csv"

# 用最乾淨、沒有任何判斷式的方式直接讀取網址
try:
    df = pd.read_csv(DATA_URL)
except Exception as e:
    st.error(f"❌ 讀取雲端資料失敗，請檢查網址。錯誤訊息: {e}")
    st.stop()

# =======================================================

if df is not None and not df.empty:
    # 清理欄位名稱空白
    df.columns = [str(c).strip() for c in df.columns]
    
    try:
        # 1. 把法人買超數字加上千分位逗號（看起來比較舒服）
        if "法人買超(張)" in df.columns:
            df["買超數字"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            df["法人買超(張)"] = df["買超數字"].apply(lambda x: f"{x:,.0f}")
        else:
            df["買超數字"] = 0

        ma5_col = "5日均價(MA5)" if "5日均價(MA5)" in df.columns else "5日均價"

        # 2. 鎖定最新交易日
        latest_date = df["日期"].iloc[0] if "日期" in df.columns else "監控清單"
        if "日期" in df.columns:
            today_df = df[df["日期"] == latest_date].copy()
        else:
            today_df = df.copy()

        # 3. 自動挑出買超前 3 名 (Top 3)
        top3 = today_df.sort_values(by="買超數字", ascending=False).head(3).copy()

        # --- 畫面呈現 ---
        st.markdown(f"### 📅 當前監控交易日：{latest_date}")
        
        # ----- TOP 3 區塊 -----
        st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
        top3_cols = [c for c in ["股票代號", "股票名稱", "關鍵分點", "法人買超(張)", "目前現價", ma5_col, "價差%", "推薦等級"] if c in top3.columns]
        if not top3.empty:
            st.dataframe(top3[top3_cols], use_container_width=True, hide_index=True)
        
        st.write("---")
        
        # ----- 全標的一覽 -----
        st.markdown(f"### 📋 {latest_date} 全標的監控清單")
        all_cols = ["日期", "股票代號", "股票名稱", "關鍵分點", "法人買超(張)", ma5_col, "目前現價", "價差%", "出現天數", "連續出現天數", "集保人數變動", "最佳購買日期", "推薦等級", "超盤建議"]
        display_cols = [c for c in all_cols if c in today_df.columns]
        
        st.dataframe(today_df[display_cols], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"💥 資料整理時發生錯誤: {e}")
