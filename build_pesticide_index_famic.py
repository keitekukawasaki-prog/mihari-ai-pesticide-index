"""
FAMIC（独立行政法人 農林水産消費安全技術センター）が公開する農薬登録情報CSVを
直接ダウンロードして、「作物×病害虫 → 登録農薬」の検索用インデックスを構築するバッチスクリプト。

【WAGRI API方式からの変更点】
WAGRIの農薬情報取得APIは「農薬番号を1件指定→詳細を返す」設計のため、
作物×病害虫のクロス検索インデックスを作るには数千回のAPI呼び出しが必要だった。
FAMICは同じ登録データを、最初から全件まとまったCSVとして無料・無制限で配布しているため、
本スクリプトはそちらを直接ダウンロードする方式に変更した。
- 申請・登録：不要
- 料金：無料
- 利用制限：なし
- 更新頻度：新規登録は登録日の翌々日、失効等は月初（月2〜3回程度更新される）

【出力】
pesticide_index.json … みはりAIアプリの MOCK_PESTICIDE_DB と同じ構造
  { "病害虫名|作物名": [{ "name": ..., "reg_no": ..., "scope": ..., "updated": ... }, ...] }

【運用】
月2回程度（FAMICの更新頻度に合わせて）cron 等で本スクリプトを実行し、
出力された pesticide_index.json をみはりAIのバックエンドが参照するDB/ファイルに反映する。

【要確認事項（実データで必ず検証すること）】
- CSVの文字コードはShift-JIS（CP932）である可能性が高い（要確認、下記ENCODINGで調整）
- 列名・列順は下記 COLUMN 定数で仮定しているが、実ファイルを開いて実際のヘッダーと照合すること
- 「登録基本部」と「登録適用部一/二」は「登録番号」列で結合(JOIN)する
"""

import csv
import io
import json
import time
import unicodedata
import zipfile
from datetime import datetime

import requests

# ---- FAMICダウンロードURL ----
# 実データで動作確認済み（2026年8月時点）。ページ更新のたびにファイル名（R0808050.zip等）が変わるため、
# 実行前に https://www.acis.famic.go.jp/ddata/index2.htm/1000 で最新のリンクを確認し、必要なら以下を更新すること。
BASE_INFO_ZIP_URL = "https://www.acis.famic.go.jp/ddata/datacsv/R0808050.zip"   # 登録基本部
APPLY_PART1_ZIP_URL = "https://www.acis.famic.go.jp/ddata/datacsv/R0808051.zip"  # 登録適用部一（登録番号52〜22553）
APPLY_PART2_ZIP_URL = "https://www.acis.famic.go.jp/ddata/datacsv/R0808052.zip"  # 登録適用部二（登録番号22554〜）

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

OUTPUT_PATH = "pesticide_index.json"

ENCODING = "cp932"  # 実データで確認済み

# みはりAIで扱っている作物名（カタカナ表記）→ FAMIC CSV上の表記ゆれを吸収するマッピング。
# 実データ確認済み。「かんきつ」はグループ登録が多く、「かんきつ(○○を除く)」のように
# 特定品目を除外した表記が頻出するため、CITRUS_GROUP_NAMES と組み合わせて判定する。
CROP_NAME_MAP = {
    "水稲": ["水稲", "水稲(陸稲を除く)", "移植水稲", "直播水稲", "湛水直播水稲", "乾田直播水稲", "稲"],
    "キュウリ": ["きゅうり"],
    "トマト": ["トマト", "とまと"],
    "ナス": ["なす"],
    "ピーマン": ["ピーマン", "ピーマン類"],
    "ブロッコリー": ["ブロッコリー"],
    "キャベツ": ["キャベツ", "かんらん"],
    "レタス": ["レタス", "ちしゃ"],
    "ホウレンソウ": ["ほうれんそう"],
    "ダイコン": ["だいこん"],
    "タマネギ": ["たまねぎ"],
    "ジャガイモ": ["ばれいしょ"],
    "スイカ": ["すいか"],
    "メロン": ["メロン", "まくわうり"],
    "イチゴ": ["いちご"],
    "空芯菜": ["えんさい", "空心菜"],
    "オリーブ": ["オリーブ"],
    "ブドウ": ["ぶどう"],
    "カキ": ["かき"],
    "モモ": ["もも", "核果類"],
    "ナシ": ["なし"],
    "リンゴ": ["りんご"],
    "キウイフルーツ": ["きうい"],
    "クリ": ["くり"],
    "ウメ": ["うめ"],
}

# 「かんきつ」グループ登録の対象となる、みはりAI側のかんきつ系作物と、CSV上での個別品目名。
# かんきつグループの行を見たとき、この個別品目名が「除く」対象に含まれていなければ対象作物とみなす。
CITRUS_CROPS = {
    "スダチ": "すだち",
    "ミカン": "温州みかん",
    "ユズ": "ゆず",
    "レモン": "レモン",
}

# みはりAI側の病害虫名（防除所の発生予察等で使われる呼び方）→ FAMIC CSV上の実際の表記。
# 完全に同じ言葉とは限らない（例：「斑点米カメムシ類」という現象名に対し、FAMICの登録データは
# 分類上の「カメムシ類」という名称で農薬を登録している）ため、別名テーブルで橋渡しする。
# 実データを見ながら継続的に追記していく。
PEST_ALIAS = {
    "斑点米カメムシ類": ["カメムシ類", "斑点米カメムシ類"],
}


def canonical_pest(famic_pest):
    """FAMICの病害虫表記から、みはりAI側の正規名（あれば）に変換する。"""
    for our_pest, variants in PEST_ALIAS.items():
        if famic_pest in variants:
            return our_pest
    return famic_pest


def crop_matches(crop_raw, our_crop):
    """CSVの作物名(crop_raw)が、みはりAIの作物(our_crop)に該当するか判定する。"""
    # 1. 通常の作物名マッピング（完全一致 or 先頭一致で品種違いも拾う）
    for alias in CROP_NAME_MAP.get(our_crop, []):
        if crop_raw == alias or crop_raw.startswith(alias + "("):
            return True

    # 2. かんきつ類は「かんきつ」グループ登録＋除外表記を判定
    if our_crop in CITRUS_CROPS:
        individual_name = CITRUS_CROPS[our_crop]
        if crop_raw == individual_name or crop_raw.startswith(individual_name + "("):
            return True  # 個別品目としての登録
        if crop_raw.startswith("かんきつ"):
            # 括弧内の「除く」リストに自分の品目名が入っていれば対象外
            if "(" in crop_raw and "を除く" in crop_raw:
                excluded_part = crop_raw[crop_raw.index("(") + 1: crop_raw.rindex(")")]
                if individual_name in excluded_part:
                    return False
            return True  # グループ登録の対象（除外されていない）
    return False


def download_zip_csv(url):
    """ZIPをダウンロードし、中のCSVをテキストとして返す（複数CSVが入っている場合は全結合）。"""
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=120)
    resp.raise_for_status()
    texts = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                raw = zf.read(name)
                texts.append(raw.decode(ENCODING, errors="replace"))
    return "\n".join(texts)


def load_base_info():
    """登録基本部: 登録番号 -> {農薬の種類名, 農薬の名称, 登録を有する者の名称}"""
    text = download_zip_csv(BASE_INFO_ZIP_URL)
    reader = csv.DictReader(io.StringIO(text))
    base = {}
    for row in reader:
        # 実ファイルの列名に合わせて要調整（想定列名を仮置き）
        reg_no = (row.get("登録番号") or "").strip()
        if not reg_no:
            continue
        base[reg_no] = {
            "type": (row.get("農薬の種類") or "").strip(),
            "name": (row.get("農薬の名称") or "").strip(),
            "holder": (row.get("登録を有する者の名称") or "").strip(),
        }
    print(f"登録基本部: {len(base)}件")
    return base


def load_apply_rows():
    """登録適用部一・二を結合して読み込む。各行は1つの (登録番号, 作物名, 適用病害虫雑草名) の組。
    FAMICは病害虫名等を半角カタカナで記録しているため、全角に正規化（NFKC）してからアプリ側の
    表記（全角カタカナ）と突き合わせる。"""
    rows = []
    for url in (APPLY_PART1_ZIP_URL, APPLY_PART2_ZIP_URL):
        text = download_zip_csv(url)
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            reg_no = (row.get("登録番号") or "").strip()
            crop = unicodedata.normalize("NFKC", (row.get("作物名") or "").strip())
            pest = unicodedata.normalize("NFKC", (row.get("適用病害虫雑草名") or "").strip())
            if not (reg_no and crop and pest):
                continue
            rows.append({"reg_no": reg_no, "crop_raw": crop, "pest": pest})
    print(f"登録適用部（一+二）: {len(rows)}行")
    return rows


ALL_OUR_CROPS = list(CROP_NAME_MAP.keys()) + list(CITRUS_CROPS.keys())


def main():
    base = load_base_info()
    apply_rows = load_apply_rows()

    index = {}
    updated_at = datetime.now().strftime("%Y年%m月時点")
    matched = 0

    for row in apply_rows:
        info = base.get(row["reg_no"])
        if not info:
            continue

        for our_crop in ALL_OUR_CROPS:
            if not crop_matches(row["crop_raw"], our_crop):
                continue
            pest_name = canonical_pest(row["pest"])
            key = f"{pest_name}|{our_crop}"
            index.setdefault(key, [])
            # 同じ農薬が同じキーに複数回入らないよう重複排除
            if not any(e["reg_no"].endswith(row["reg_no"]) for e in index[key]):
                index[key].append({
                    "name": info["name"],
                    "reg_no": f"登録第{row['reg_no']}号",
                    "scope": f"{our_crop}・{pest_name}",
                    "updated": updated_at,
                })
                matched += 1

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"マッチ件数: {matched}")
    print(f"索引キー数: {len(index)}")
    print(f"書き出し完了: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
