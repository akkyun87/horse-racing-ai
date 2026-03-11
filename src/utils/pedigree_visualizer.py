import os
import sqlite3
import subprocess

# --- 設定エリア ---
DB_PATH = os.path.join("data", "raw", "pedigree", "pedigree.db")
HTML_DIR = os.path.join("images", "pedigree", "html")
IMG_DIR = os.path.join("images", "pedigree", "img")
# wkhtmltoimageのパス（環境に合わせて適宜修正してください）
WKHTMLTOIMAGE = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe"


def get_pedigree_data(horse_id):
    """
    DBから血統データを取得し、[父系31頭, 母系31頭] の構造を
    家系図の[a0〜a61]インデックスに正しくマッピングする
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # IDを0埋め10桁の文字列に揃える
    target_id = str(horse_id).zfill(10)

    cur.execute(
        "SELECT name, five_gen_ancestors FROM pedigree_info WHERE horse_id=?",
        (target_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        print(f"Error: horse_id {target_id} not found.")
        return None, None

    name = row[0]
    # 全62頭（または63頭）のリストを取得
    raw_list = [x.strip() for x in row[1].split(",") if x.strip()]

    # 先頭に対象馬自身が含まれている場合は除去して62頭にする
    if len(raw_list) == 63:
        raw_list = raw_list[1:]

    if len(raw_list) < 62:
        print(f"Warning: Data insufficient for {name} (Count: {len(raw_list)})")
        while len(raw_list) < 62:
            raw_list.append("")

    # --- インデックス再マッピング ---
    # raw_list[0:31] = 父系 (1代x1, 2代x2, 3代x4, 4代x8, 5代x16)
    # raw_list[31:62] = 母系 (1代x1, 2代x2, 3代x4, 4代x8, 5代x16)

    p_sire = raw_list[0:31]
    p_dam = raw_list[31:62]

    # 出力用の配列 a (0〜61) を作成
    # a[0]=父, a[1]=母, a[2]=父父, a[3]=父母, a[4]=母父, a[5]=母母 ... の順
    a = [None] * 62

    # 1代前 (1頭ずつ)
    a[0], a[1] = p_sire[0], p_dam[0]
    # 2代前 (2頭ずつ)
    a[2:4], a[4:6] = p_sire[1:3], p_dam[1:3]
    # 3代前 (4頭ずつ)
    a[6:10], a[10:14] = p_sire[3:7], p_dam[3:7]
    # 4代前 (8頭ずつ)
    a[14:22], a[22:30] = p_sire[7:15], p_dam[7:15]
    # 5代前 (16頭ずつ)
    a[30:46], a[46:62] = p_sire[15:31], p_dam[15:31]

    return name, a


def build_html(name, a):
    """HTMLテーブル構造の組み立て (rowspanロジック)"""
    html_body = f"""
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: "MS Gothic", "Meiryo", Arial, sans-serif; margin: 10px; }}
        h2 {{ font-size: 18px; color: #333; border-bottom: 2px solid #333; display: inline-block; }}
        table {{ border-collapse: collapse; border: 2px solid #000; table-layout: fixed; width: 1100px; }}
        td {{ 
            border: 1px solid #444; 
            height: 26px; 
            padding: 2px 5px; 
            font-size: 11px; 
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }}
        .target {{ background: #eee; font-weight: bold; text-align: center; width: 120px; }}
        .sire-side {{ background: #f0faff; }} /* 父系カラー */
        .dam-side {{ background: #fff5f7; }}  /* 母系カラー */
    </style>
</head>
<body>
    <h2>{name} 5代血統表</h2>
    <table>
        <tr>
            <td class="target" rowspan="32">{name}</td>
            <td class="sire-side" rowspan="16">{a[0]}</td>
            <td rowspan="8">{a[2]}</td>
            <td rowspan="4">{a[6]}</td>
            <td rowspan="2">{a[14]}</td>
            <td>{a[30]}</td>
        </tr>
        <tr><td>{a[31]}</td></tr>
        <tr><td rowspan="2">{a[15]}</td><td>{a[32]}</td></tr>
        <tr><td>{a[33]}</td></tr>
        <tr><td rowspan="4">{a[7]}</td><td rowspan="2">{a[16]}</td><td>{a[34]}</td></tr>
        <tr><td>{a[35]}</td></tr>
        <tr><td rowspan="2">{a[17]}</td><td>{a[36]}</td></tr>
        <tr><td>{a[37]}</td></tr>
        <tr><td rowspan="8">{a[3]}</td><td rowspan="4">{a[8]}</td><td rowspan="2">{a[18]}</td><td>{a[38]}</td></tr>
        <tr><td>{a[39]}</td></tr>
        <tr><td rowspan="2">{a[19]}</td><td>{a[40]}</td></tr>
        <tr><td>{a[41]}</td></tr>
        <tr><td rowspan="4">{a[9]}</td><td rowspan="2">{a[20]}</td><td>{a[42]}</td></tr>
        <tr><td>{a[43]}</td></tr>
        <tr><td rowspan="2">{a[21]}</td><td>{a[44]}</td></tr>
        <tr><td>{a[45]}</td></tr>
        <tr>
            <td class="dam-side" rowspan="16">{a[1]}</td>
            <td rowspan="8">{a[4]}</td>
            <td rowspan="4">{a[10]}</td>
            <td rowspan="2">{a[22]}</td>
            <td>{a[46]}</td>
        </tr>
        <tr><td>{a[47]}</td></tr>
        <tr><td rowspan="2">{a[23]}</td><td>{a[48]}</td></tr>
        <tr><td>{a[49]}</td></tr>
        <tr><td rowspan="4">{a[11]}</td><td rowspan="2">{a[24]}</td><td>{a[50]}</td></tr>
        <tr><td>{a[51]}</td></tr>
        <tr><td rowspan="2">{a[25]}</td><td>{a[52]}</td></tr>
        <tr><td>{a[53]}</td></tr>
        <tr><td rowspan="8">{a[5]}</td><td rowspan="4">{a[12]}</td><td rowspan="2">{a[26]}</td><td>{a[54]}</td></tr>
        <tr><td>{a[55]}</td></tr>
        <tr><td rowspan="2">{a[27]}</td><td>{a[56]}</td></tr>
        <tr><td>{a[57]}</td></tr>
        <tr><td rowspan="4">{a[13]}</td><td rowspan="2">{a[28]}</td><td>{a[58]}</td></tr>
        <tr><td>{a[59]}</td></tr>
        <tr><td rowspan="2">{a[29]}</td><td>{a[60]}</td></tr>
        <tr><td>{a[61]}</td></tr>
    </table>
</body>
</html>
"""
    return html_body


def generate_pedigree_image(horse_id):
    """実行メイン関数"""
    os.makedirs(HTML_DIR, exist_ok=True)
    os.makedirs(IMG_DIR, exist_ok=True)

    # 1. データ取得と並び替え
    name, ancestors = get_pedigree_data(horse_id)
    if not name:
        return

    # 2. HTML生成
    html = build_html(name, ancestors)

    # 3. ファイル保存
    safe_id = str(horse_id).zfill(10)
    html_path = os.path.join(HTML_DIR, f"{safe_id}.html")
    img_path = os.path.join(IMG_DIR, f"{safe_id}.png")

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    # 4. PNG変換
    try:
        subprocess.run(
            [
                WKHTMLTOIMAGE,
                "--width",
                "1150",
                "--disable-smart-width",
                html_path,
                img_path,
            ],
            check=True,
            capture_output=True,
        )
        print(f"Success: {img_path}")
    except Exception as e:
        print(f"Error during image conversion: {e}")


if __name__ == "__main__":
    # python -m utils.pedigree_visualizer
    # テスト実行: クロワデュノール
    generate_pedigree_image("0001352760")
