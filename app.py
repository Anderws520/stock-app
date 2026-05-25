# ===== Python Streamlit 終極收官完美版 (自動 URL 編碼防呆) =====
import streamlit as st
import pandas as pd
import urllib.parse

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# ==================== [ 關鍵設定區 ] ====================
# 🔴 請務必將下方的字串換成你 Google 試算表網址列上的那一串長代碼！
SPREADSHEET_ID = "1A2B3C4D5E6F7G8H9I0J" 
SHEET_NAME = "stock_Sheet"
# =======================================================

# 安全處理：將中文字的工作表名稱轉為網址看得懂的編碼 (避免 400 Bad Request)
encoded_sheet_name = urllib.parse.quote(SHEET_NAME)
csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"

@st.cache_data(ttl=60)  # 快取 1 分鐘，方便你測試時即時看動態
def load_data(url):
    try:
        # 強制將關鍵欄位當字串讀取，防止股票代號開頭的 0 被吃掉
        return pd.read_csv(url, dtype={"日期": str, "股票代號": str})
    except Exception as e:
        st.error(f"❌ 從 Google Sheets 讀取資料失敗: {e}")
        return None

# 讀取資料
df = load_data(csv_url)

# --- 資料處理與畫面呈現 ---
if df is None or df.empty:
    st.warning("⚠️ 讀取到的分頁目前沒有資料，或尚未開啟共用權限。請確認試算表已開啟「 know link 的任何人都能檢視 」，且 ID 填寫正確。")
else:
    # 1. 欄位名稱極致清理（防止隱形空白或特殊字元干擾）
    df.columns = [str(c).strip() for c in df.columns]
    
    # 2. 欄位安全檢查
    required_cols = ["日期", "股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "推薦等級", "操盤建議"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前缺少欄位: {missing_cols}")
        st.info("💡 偵測到你的試算表標頭可能長得不一樣，目前抓到的表頭有：\n" + ", ".join(df.columns))
    else:
        try:
            # 3. 資料型態強制轉換，避免排序崩潰
            df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            # 4. 取得最新交易日的資料
            latest_date = df["日期"].iloc[0] if not df.empty else "未知的日期"
            today_df = df[df["日期"] == latest_date].copy()
            
            # 5. 精選 Top 3 邏輯
            filtered_df = today_df[today_df["推薦等級"].str.contains("推薦|關注", na=False)].copy()
            top3 = filtered_df.sort_values(by="買超_n", ascending=False).head(3)
            
            # 6. 渲染 Streamlit 畫面
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
            st.error(f"💥 運算資料時發生非預期錯誤: {e}")
