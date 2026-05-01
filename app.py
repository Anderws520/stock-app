def download_t86(date: datetime.date):
    """下載三大法人 T86（已強制處理 SSL + 詳細除錯）"""
    if not is_trading_day(date):
        return None

    url = get_t86_url(date)
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        # 關鍵修正：強制關閉 SSL 驗證 + 增加超時
        resp = requests.get(url, headers=headers, timeout=30, verify=False)
        resp.raise_for_status()
        
        text = resp.text[:2000]  # 只取前面避免太長
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        # 找資料起始行
        start_idx = None
        for i, line in enumerate(lines):
            if "證券代號" in line or "证券代號" in line:
                start_idx = i
                break

        if start_idx is None:
            st.warning(f"{date} 無法找到『證券代號』，可能格式又變了")
            st.text_area(f"{date} 原始內容預覽", text[:800], height=150)
            return None

        csv_content = "\n".join(lines[start_idx:])
        df = pd.read_csv(StringIO(csv_content), encoding='big5', on_bad_lines='skip')

        # 清理欄位
        df.columns = [str(col).strip().replace('\n', '').replace(' ', '') for col in df.columns]

        st.caption(f"[{date}] 成功抓到欄位: {list(df.columns)[:12]}")

        # 找買賣超欄位
        buy_col = None
        for name in ['三大法人買賣超股數', '三大法人買賣超', '買賣超股數']:
            if name in df.columns:
                buy_col = name
                break

        if not buy_col or '證券代號' not in df.columns:
            st.error(f"{date} 缺少關鍵欄位")
            return None

        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df = df.dropna(subset=['證券代號', '三大法人買賣超股數']).copy()
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.strip().str.zfill(4)

        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]

    except Exception as e:
        st.error(f"{date} 下載失敗: {type(e).__name__} - {str(e)[:150]}")
        return None