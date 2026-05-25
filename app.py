import streamlit as st
import pandas as pd

st.set_page_col_config = {"layout": "wide"}
st.title("📊 三大法人籌碼監控儀表板")

# --- 終極解法：使用 Google Sheets 內建的 CSV 導出連結，完全免安裝 streamlit_gsheets ---
# 請將下方的 SPREADSHEET_ID 替換成你 Google 試算表網址列上的那一串長代碼！
SPREADSHEET_ID = "請換成你的Google試算表ID" 
SHEET_NAME = "stock_Sheet"

csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"

@st.cache_data(ttl=300)  # 每 5 分鐘自動刷新快取
def load_data(url):
    try:
        # 強制將前兩欄轉為字串，防止股票代號（如 0050）的開頭 0 被吃掉
        return pd.read_csv(url, dtype={"日期": str, "股票代號": str})
    except Exception as e:
        st.error(f"從 Google Sheets 讀取資料失敗: {e}")
        return None

# 讀取資料
df = load_data(csv_url)

# --- 核心操盤資料對齊與防呆計算 ---
if df is None or df.empty:
    st.warning("⚠️ 無法讀取資料，請確認你的 Google 試算表 ID 是否正確，且該試算表已開啟「知道連結的任何人都能檢視」權限。")
else:
    # 1. 清理欄位名稱（去除任何隱形空白、換行符）
    df.columns = [str(c).strip() for c in df.columns]
    
    # 2. 檢查關鍵欄位是否存在，若不存在則顯示友善提示
    required_cols = ["日期", "股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "推薦等級", "操盤建議"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前缺少欄位: {missing_cols}")
        st.info("請確認你的 Google 試算表第一行表頭是否已手動修改為標準欄位名稱。")
    else:
        try:
            # 3. 資料型態強制轉換，避免排序或篩選崩潰
            df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            # 4. 取得最新交易日的資料
            latest_date = df["日期"].iloc[0] if not df.empty else "未知的日期"
            today_df = df[df["日期"] == latest_date].copy()
            
            # 5. 精選 Top 3 邏輯（篩選出強烈推薦或值得關注，並按買超張數排序）
            filtered_df = today_df[today_df["推薦等級"].str.contains("推薦|關注", na=False)].copy()
            top3 = filtered_df.sort_values(by="買超_n", ascending=False).head(3)
            
            # 6. 渲染 Streamlit 美麗的前端畫面
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
