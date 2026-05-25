import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd

# 設置網頁標題與基本配置
st.set_page_config(
    page_title="三大法人籌碼監控儀表板",
    page_icon="📊",
    layout="wide"
)

st.title("📊 三大法人籌碼監控儀表板")
st.caption("自動化法人大戶籌碼追蹤系統 - 資料即時同步自 Google Sheets")

# ==========================================
# 1. 建立 Google Sheets 連線並讀取資料
# ==========================================
try:
    # 建立 gsheets 連線物件
    conn = st.connection("gsheets", type=GSheetsConnection)
    
    # 讀取指定的工作表 stock_Sheet，並設定不使用快取以利資料即時更新
    df = conn.read(worksheet="stock_Sheet", ttl=0)
    
except Exception as e:
    st.error(f"❌ 無法連線至 Google Sheets 資料來源: {e}")
    st.info("請檢查專案目錄下的 `.streamlit/secrets.toml` 是否已正確設定 Google Sheets 的共用連結 (spreadsheet_url)。")
    st.stop()

# ==========================================
# 2. 終極防呆驗證與資料清洗機制
# ==========================================
if df is None or df.empty:
    st.warning("⚠️ 順利連線，但目前 `stock_Sheet` 工作表內沒有任何資料。")
    st.info("請先至 Google Apps Script (GAS) 編輯器中執行一次 'backfillHistory' 函數來匯入資料。")
else:
    # 2.1 強制清理欄位名稱（去除任何隱形空白、換行符，確保與 GAS 完全對齊）
    df.columns = [str(c).strip() for c in df.columns]
    
    # 2.2 檢查我們所需的關鍵欄位是否存在
    required_cols = ["日期", "股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "推薦等級", "操盤建議"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前缺少關鍵欄位: {missing_cols}")
        st.info("這通常是因為試算表內存在錯位表頭。請將 Google Sheets 的 `stock_Sheet` 分頁手動刪除，然後去 GAS 點選執行 'backfillHistory' 重新建表即可修復。")
    else:
        try:
            # 2.3 資料型態安全轉換（防呆，避免將文字拿來做排序或計算）
            df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            if "價差%" in df.columns:
                df["價差%"] = pd.to_numeric(df["價差%"], errors='coerce').fillna(0)
            
            # 2.4 自動抓取最新一個交易日的日期 (假設資料流已排序，第一筆即為最新日)
            latest_date = df["日期"].iloc[0] if not df.empty else "未知的交易日"
            
            # 2.5 篩選出該最新日期的所有標的
            today_df = df[df["日期"] == latest_date].copy()
            
            # ==========================================
            # 3. 核心大戶籌碼邏輯計算 (Top 3 精選)
            # ==========================================
            # 篩選今日數據中，推薦等級包含「推薦」或「關注」的股票（排除謹蹤或普通股）
            filtered_df = today_df[today_df["推薦等級"].str.contains("推薦|關注", na=False)].copy()
            
            # 根據剛才轉好的「買超_n」欄位進行由大到小的排序，並取出前三名
            top3 = filtered_df.sort_values(by="買超_n", ascending=False).head(3)
            
            # ==========================================
            # 4. 渲染 Streamlit 前端網頁畫面
            # ==========================================
            # 顯示當前盯盤的日期
            st.info(f"📅 當前監控交易日：{latest_date} (資料已對齊最新法人買賣超)")
            
            # 側邊欄區塊：提供快速查看基礎統計
            st.sidebar.markdown("### 📊 盤後數據統計")
            st.sidebar.metric(label="今日監控總標的數", value=f"{len(today_df)} 檔")
            st.sidebar.metric(label="法人重點關注檔數", value=f"{len(filtered_df)} 檔")
            
            # 區塊 A：今日最強 Top 3 精選標的
            st.markdown("---")
            st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
            
            if not top3.empty:
                # 漂亮呈現 Top 3 清單
                st.dataframe(
                    top3[["股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "推薦等級", "操盤建議"]],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("💡 提示：今日暫無符合「強烈推薦」或「值得關注」標準的 Top 3 大戶爆買標的。")
            
            # 區塊 B：全交易日大清單
            st.markdown("---")
            st.markdown(f"### 📋 {latest_date} 全標的監控清單 (買超 ≥ 500張)")
            
            # 完整呈現今日所有抓到的股票
            display_cols = ["股票代號", "股票名稱", "法人買超(張)", "目前現價", "5日均價(MA5)", "建議買價", "预估目標價", "推薦等級", "操盤建議"]
            # 防呆：確保要顯示的欄位在今日 DataFrame 中都有，沒有的就自動忽略
            available_display_cols = [c for c in display_cols if c in today_df.columns]
            
            st.dataframe(
                today_df[available_display_cols],
                use_container_width=True,
                hide_index=True
            )
            
        except Exception as e:
            st.error(f"💥 運算籌碼資料時發生非預期錯誤: {e}")
            st.info("建議檢查 Google 試算表內特定欄位的儲存格內容是否有文字與數字混雜的情況。")
