import streamlit as st
import pandas as pd

# 假設這裡是你目前從 Google Sheets 連線讀進來的 DataFrame
# df = conn.read(...)

# 1. 欄位名稱防呆處理：強制轉字串並清除前後空白（修正語法，補齊括號）
df.columns = [str(c).strip() for c in df.columns]

# 2. 核心操盤資料對齊與防呆計算
try:
    # 將「法人買超(張)」與「價差%」轉換為數值型態，避免文字排序錯誤
    df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce')
    if "價差%" in df.columns:
        df["價差%"] = pd.to_numeric(df["價差%"], errors='coerce')
    
    # 篩選今日數據 (假設取最新日期的資料)
    latest_date = df["日期"].iloc[0] if not df.empty else ""
    today_df = df[df["日期"] == latest_date].copy()
    
    # 篩選出有主力進駐（強烈推薦或值得關注）的標的
    filtered_df = today_df[today_df["推薦等級"].str.contains("推薦|關注", na=False)].copy()
    
    # 精選 Top 3 標的排序（依據剛才轉換完的 買超_n 排序）
    top3 = filtered_df.sort_values(by="買超_n", ascending=False).head(3)
    
    # 3. 在 Streamlit 網頁畫面上漂亮呈現
    st.title(f"📊 {latest_date} 三大法人籌碼監控儀表板")
    
    st.subheader("🏆 今日法人大戶爆買精選 Top 3")
    if not top3.empty:
        st.dataframe(top3[["股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "操盤建議"]])
    else:
        st.info("今日暫無符合強烈推薦標準的 Top 3 標的。")

    st.write("---")
    st.subheader(f"📋 {latest_date} 全標的監控清單 (買超 ≥ 500張)")
    st.dataframe(today_df[["股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "建議買價", "預估目標價", "推薦等級", "操盤建議"]])

except KeyError as e:
    st.error(f"❌ 欄位對齊失敗，找不到欄位: {e}")
    st.info("請確認你的 Google 試算表第一行標頭是否已成功更新。")
except Exception as e:
    st.error(f"運行錯誤: {e}")
