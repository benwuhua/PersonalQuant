from __future__ import annotations

from datetime import datetime, timedelta
import io
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


EASTMONEY_LIST_URL = 'https://np-anotice-stock.eastmoney.com/api/security/ann'
PDF_URL_TEMPLATE = 'https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf'
USER_AGENT = (
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
)
FALLBACK_MARKER = '公告栏目：'
GARBLED_PATTERN = re.compile(r'[�ͪٙዄ΂Оப΂ʮ̡]{6,}')


def load_sample_announcements(path: Path) -> list[dict]:
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def instrument_to_stock_code(instrument: str) -> str:
    return ''.join(ch for ch in instrument if ch.isdigit())


def stock_code_to_instrument(stock_code: str, market_code: str | None = None) -> str:
    if stock_code.startswith('6'):
        return f'SH{stock_code}'
    if stock_code.startswith(('0', '3')):
        return f'SZ{stock_code}'
    if market_code == '1':
        return f'SH{stock_code}'
    return f'SZ{stock_code}'


def build_pdf_url(art_code: str) -> str:
    return PDF_URL_TEMPLATE.format(art_code=art_code)


def parse_notice_date(value: str) -> datetime:
    return datetime.strptime(value[:10], '%Y-%m-%d')


def shanghai_today() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)


def clean_extracted_text(text: str, max_chars: int) -> str:
    text = text.replace('\x00', ' ')
    text = text.replace('\r', '\n')
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    text = re.sub(r'([。；！？])\s*', r'\1\n', text)
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if len(line) == 1 and not line.isdigit():
            continue
        lines.append(line)
    merged = '\n'.join(lines)
    merged = re.sub(r'\n{3,}', '\n\n', merged).strip()
    if len(merged) > max_chars:
        merged = merged[:max_chars].rstrip()
    return merged


def assess_text_quality(text: str) -> dict[str, Any]:
    if not text:
        return {'quality_score': 0.0, 'is_usable': False, 'reason': 'empty'}
    length = len(text)
    cjk_chars = sum(1 for ch in text if '\u4e00' <= ch <= '\u9fff')
    ascii_letters = sum(1 for ch in text if ch.isascii() and ch.isalpha())
    digits = sum(1 for ch in text if ch.isdigit())
    weird_chars = sum(1 for ch in text if ord(ch) > 127 and not ('\u4e00' <= ch <= '\u9fff'))
    line_count = max(text.count('\n') + 1, 1)
    cjk_ratio = cjk_chars / max(length, 1)
    weird_ratio = weird_chars / max(length, 1)
    alnum_ratio = (cjk_chars + ascii_letters + digits) / max(length, 1)
    garbled_hits = len(GARBLED_PATTERN.findall(text))

    score = 0.0
    if length >= 120:
        score += 0.25
    if line_count >= 6:
        score += 0.15
    if cjk_ratio >= 0.12:
        score += 0.25
    if alnum_ratio >= 0.45:
        score += 0.20
    if weird_ratio <= 0.30:
        score += 0.15
    if garbled_hits:
        score -= 0.35
    score = max(0.0, min(1.0, round(score, 4)))

    is_usable = score >= 0.45 and garbled_hits == 0 and not text.startswith(FALLBACK_MARKER)
    reason = 'ok' if is_usable else 'low_quality_pdf_text'
    return {
        'quality_score': score,
        'is_usable': is_usable,
        'reason': reason,
        'length': length,
        'cjk_ratio': round(cjk_ratio, 4),
        'weird_ratio': round(weird_ratio, 4),
        'garbled_hits': garbled_hits,
    }


def extract_pdf_text(pdf_url: str, session: requests.Session, max_chars: int) -> str:
    if PdfReader is None:
        return ''
    response = session.get(pdf_url, timeout=30)
    response.raise_for_status()
    reader = PdfReader(io.BytesIO(response.content))
    chunks: list[str] = []
    total = 0
    for page in reader.pages[:12]:
        text = (page.extract_text() or '').strip()
        if not text:
            continue
        cleaned = clean_extracted_text(text, max_chars=max_chars)
        if not cleaned:
            continue
        remain = max_chars - total
        if remain <= 0:
            break
        piece = cleaned[:remain]
        chunks.append(piece)
        total += len(piece)
    return clean_extracted_text('\n\n'.join(chunks), max_chars=max_chars)


def build_fallback_content(title: str, column_names: list[str]) -> str:
    return f"公告栏目：{'、'.join(column_names) if column_names else '未分类'}；公告标题：{title}"


def build_announcement_content(title: str, column_names: list[str], pdf_text: str) -> tuple[str, str, dict[str, Any]]:
    fallback = build_fallback_content(title, column_names)
    cleaned_pdf_text = clean_extracted_text(pdf_text, max_chars=max(len(pdf_text), 1)) if pdf_text else ''
    quality = assess_text_quality(cleaned_pdf_text)
    if quality['is_usable'] and FALLBACK_MARKER not in cleaned_pdf_text[:20]:
        return f"公告标题：{title}\n公告正文摘录：\n{cleaned_pdf_text}", 'pdf_excerpt', quality
    return fallback, 'title_only', quality


def fetch_eastmoney_for_code(
    stock_code: str,
    lookback_days: int,
    page_size: int,
    max_pages: int,
    download_pdf_text: bool,
    pdf_text_chars: int,
    session: requests.Session,
) -> list[dict[str, Any]]:
    cutoff = shanghai_today().date() - timedelta(days=lookback_days)
    records: list[dict[str, Any]] = []

    for page_index in range(1, max_pages + 1):
        response = session.get(
            EASTMONEY_LIST_URL,
            params={
                'page_size': page_size,
                'page_index': page_index,
                'ann_type': 'A',
                'stock_list': stock_code,
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get('data', {}).get('list', [])
        if not rows:
            break

        should_stop = False
        for row in rows:
            notice_dt = parse_notice_date(row['notice_date']).date()
            if notice_dt < cutoff:
                should_stop = True
                continue

            code_info = row.get('codes', [{}])[0]
            instrument = stock_code_to_instrument(code_info.get('stock_code', stock_code), code_info.get('market_code'))
            column_names = [col['column_name'] for col in row.get('columns', []) if col.get('column_name')]
            pdf_url = build_pdf_url(row['art_code'])
            pdf_text = ''
            if download_pdf_text:
                try:
                    pdf_text = extract_pdf_text(pdf_url, session=session, max_chars=pdf_text_chars)
                except Exception:
                    pdf_text = ''
            content, content_source, quality = build_announcement_content(row['title'], column_names, pdf_text)
            records.append(
                {
                    'instrument': instrument,
                    'stock_code': code_info.get('stock_code', stock_code),
                    'short_name': code_info.get('short_name', ''),
                    'publish_date': notice_dt.isoformat(),
                    'title': row['title'],
                    'content': content,
                    'content_source': content_source,
                    'content_length': len(content),
                    'content_quality_score': quality.get('quality_score', 0.0),
                    'content_quality_reason': quality.get('reason', ''),
                    'column_names': column_names,
                    'art_code': row['art_code'],
                    'pdf_url': pdf_url,
                    'source': 'eastmoney',
                }
            )

        if should_stop:
            break

    deduped: dict[str, dict[str, Any]] = {}
    for item in records:
        deduped[item['art_code']] = item
    return list(deduped.values())


def fetch_eastmoney_announcements(candidates: pd.DataFrame, cfg: dict) -> list[dict]:
    acfg = cfg['announcements']
    lookback_days = int(acfg.get('lookback_days', 7))
    page_size = int(acfg.get('page_size', 50))
    max_pages = int(acfg.get('max_pages', 2))
    download_pdf_text = bool(acfg.get('download_pdf_text', False))
    pdf_text_chars = int(acfg.get('pdf_text_chars', 1200))
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT, 'Referer': 'https://data.eastmoney.com/'})

    all_items: list[dict] = []
    for instrument in candidates['instrument'].dropna().astype(str).tolist():
        stock_code = instrument_to_stock_code(instrument)
        try:
            items = fetch_eastmoney_for_code(
                stock_code=stock_code,
                lookback_days=lookback_days,
                page_size=page_size,
                max_pages=max_pages,
                download_pdf_text=download_pdf_text,
                pdf_text_chars=pdf_text_chars,
                session=session,
            )
        except Exception as exc:
            items = [
                {
                    'instrument': instrument,
                    'stock_code': stock_code,
                    'short_name': '',
                    'publish_date': shanghai_today().date().isoformat(),
                    'title': f'公告抓取失败: {exc}',
                    'content': f'公告抓取失败: {exc}',
                    'content_source': 'fetch_error',
                    'content_length': len(f'公告抓取失败: {exc}'),
                    'content_quality_score': 0.0,
                    'content_quality_reason': 'fetch_error',
                    'column_names': ['抓取异常'],
                    'art_code': f'error-{instrument}',
                    'pdf_url': '',
                    'source': 'eastmoney',
                }
            ]
        all_items.extend(items)

    return sorted(all_items, key=lambda x: (x['publish_date'], x['instrument'], x['art_code']), reverse=True)


def fetch_announcements_for_candidates(candidates: pd.DataFrame, cfg: dict) -> list[dict]:
    acfg = cfg['announcements']
    source = acfg.get('source', 'eastmoney')
    if source == 'sample':
        root = Path(__file__).resolve().parents[2]
        all_items = load_sample_announcements(root / acfg['input_file'])
        target_codes = set(candidates['instrument'].tolist())
        return [item for item in all_items if item['instrument'] in target_codes]
    if source == 'eastmoney':
        return fetch_eastmoney_announcements(candidates, cfg)
    raise NotImplementedError(f'不支持的公告源: {source}')
