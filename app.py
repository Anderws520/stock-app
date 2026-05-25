import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title(" 📊 三大法人籌碼監控儀表板")

# ===== 終極分頁 ID 雷射綁定法 =====
SPREADSHEET_ID = "1GjcN6DSFWwJG14bPyMW8aNUkE70Auz6BQFPJ9EGzR38"
# 鎖定包含「日期、股票代號、股票名稱、法人買超(張)...」的正確分頁 GID
GID = "643915918"  

# 透過 Google Sheets 網頁端原生的 CSV 導出端點獲取資料，完全不用裝額外套件
csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid={GID}"

@st.cache_data(ttl=30)  # 快取設為 30 秒，方便即時排錯調試
def load_data(url):
    try:
        # 強制指定前兩欄為字串型態，嚴防 00403A、02001R 等特殊代號的開頭 0 被吃掉
        return pd.read_csv(url, dtype={"日期": str, "股票代號": str})
    except Exception as e:
        st.error(f"❌ 無法從 Google 試算表指定分頁讀取資料: {e}")
        return None

df = load_data(csv_url)

# --- 核心資料處理與對齊 ---
if df is None or df.empty:
    st.warning("⚠️ 讀取到的分頁目前沒有資料，或尚未開啟共用權限。請確認試算表已開啟「知道連結的任何人都能檢視」。")
else:
    # 1. 欄位名稱去除看不見的空白字元
    df.columns = [str(c).strip() for c in df.columns]
    
    # 2. 嚴格檢查這 8 個在 Streamlit 畫面上必須呈現的黃金核心欄位
    required_cols = ["日期", "股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "推薦等級", "操盤建議"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前讀取到的欄位為: {list(df.columns)}")
        st.info(f"缺少了必要的欄位: {missing_cols}，請檢查你的 Google 試算表第一行（Row 1）表頭文字是否正確。")
    else:
        try:
            # 3. 資料型態安全轉換，杜絕所有排序或篩選時的 KeyError
            df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            # 4. 撈出試算表中最頂部（最新交易日）的資料
            latest_date = df["日期"].dropna().iloc[0] if not df.empty else "無資料日期"
            today_df = df[df["日期"] == latest_date].copy()
            
            # 5. 精選 Top 3 邏輯（只抓「推薦等級」裡有包含推薦、關注星星的股票，並按買超張數排序）
            filtered_df = today_df[today_df["推薦等級"].astype(str).str.contains("推薦|關注", na=False)].copy()
            top3 = filtered_df.sort_values(by="買超_n", ascending=False).head(3)
            
            # 6. Streamlit 前端網頁精緻呈現
            st.markdown(f"### 📅 當前監控交易日：{latest_date}")
            
            st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
            if not top3.empty:
                st.dataframe(
                    top3[["股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "操盤建議"]],
                    use_container_width=True
                )
            else:
                st.info("💡 今日暫無符合強烈推薦或值得關注標準的 Top 3 標的。")
            
            st.write("---")
            st.markdown(f"### 📋 {latest_date} 全標的監控清單 (買超 ≥ 500張)")
            st.dataframe(
                today_df[["股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "建議買價", "預估目標價", "推薦等級", "操盤建議"]],
                use_container_width=True
            )
            
        except Exception as e:
            st.error(f"💥 運行資料時發生非預期錯誤: {e}")
