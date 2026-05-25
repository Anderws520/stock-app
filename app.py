# ===== Python Streamlit 欄位完全還原與格式優化版 =====
import streamlit as st
import pandas as pd
import urllib.parse

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# ==================== [ 關鍵設定區 ] ====================
# 🔴 請確保此處的 ID 與你的 Google 試算表一致
SPREADSHEET_ID = "請在此處貼上你的Google試算表ID" 
SHEET_NAME = "stock_Sheet"
# =======================================================

# 安全網址處理
encoded_sheet_name = urllib.parse.quote(SHEET_NAME)
csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"

@st.cache_data(ttl=60)  # 快取 1 分鐘，測試時更新最即時
def load_data(url):
    try:
        # 強制將關鍵欄位當字串讀取，防止股票代號開頭的 0 或 A 被吃掉
        return pd.read_csv(url, dtype={"日期": str, "股票代號": str})
    except Exception as e:
        st.error(f"❌ 從 Google Sheets 讀取資料失敗: {e}")
        return None

# 讀取資料
df = load_data(csv_url)

if df is None or df.empty:
    st.warning("⚠️ 讀取到的分頁目前沒有資料，或尚未開啟共用權限。")
else:
    # 1. 欄位名稱極致清理（防止隱形空白或特殊字元干擾）
    df.columns = [str(c).strip() for c in df.columns]
    
    # 【還原你原本的所有欄位】檢查
    required_cols = ["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "出現天數", "集保人數變動", "最佳購買日期", "超盤建議"]
    
    # 2. 自動容錯：如果試算表內寫的是「法人買超(張)」或「操盤建議」，自動校正名稱對齊
    rename_dict = {
        "法人買超(張)": "買超張數",
        "5日均價(MA5)": "5日均價",
        "操盤建議": "超盤建議",
        "推薦等級": "集保人數變動"
    }
    df.rename(columns=rename_dict, inplace=True)
    df.columns = [str(c).strip() for c in df.columns]

    # 再次檢查哪些真的不見了
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前缺少欄位: {missing_cols}")
        st.info("💡 目前抓到的表頭有：\n" + ", ".join(df.columns))
    else:
        try:
            # 3. 資料型態強制轉換，避免排序崩潰
            df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            # 4. 價差% 格式化處理（小數 0.06 轉成 6.00%）
            df["價差%_n"] = pd.to_numeric(df["價差%"], errors='coerce').fillna(0)
            df["價差%"] = df["價差%_n"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
            
            # 5. 取得最新交易日的資料
            latest_date = df["日期"].iloc[0] if not df.empty else "未知的日期"
            today_df = df[df["日期"] == latest_date].copy()
            
            # 6. 精選 Top 3 邏輯（按買超張數排序）
            top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3)
            
            # 7. 渲染 Streamlit 畫面
            st.markdown(f"### 📅 當前監控交易日：{latest_date}")
            
            # ----- TOP 3 區塊 顯示你原本要的完整資訊 -----
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
            
            # ----- 全標的一覽 呈現你試算表所有的精華欄位 -----
            st.markdown(f"### 📋 {latest_date} 全標的監控清單 (買超 ≥ 500張)")
            st.dataframe(
                today_df[["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "出現天數", "集保人數變動", "最佳購買日期", "超盤建議"]],
                use_container_width=True,
                hide_index=True
            )
            
        except Exception as e:
            st.error(f"💥 運算資料時發生非預期錯誤: {e}")
