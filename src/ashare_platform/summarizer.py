from __future__ import annotations

from collections import Counter
import json
import os
import re
from typing import Any

import requests

KEYWORDS = {
    '业绩': ['业绩', '预增', '预减', '快报', '年报', '季报'],
    '分红回购': ['回购', '增持', '分红'],
    '融资并购': ['定增', '收购', '重组', '并购', '募资'],
    '风险事项': ['诉讼', '问询', '减持', '处罚', '风险提示'],
    '经营进展': ['订单', '中标', '投产', '合作', '项目'],
}

LOW_VALUE_KEYWORDS = [
    '薪酬',
    '会议资料',
    '董事会决议',
    '监事会决议',
    '关于召开',
    'H股公告',
    '审计报告',
    '独立董事述职报告',
    '内部控制自我评价报告',
    'ESG报告',
    '股东会的通知',
    '股东大会的通知',
    '年度报告',
    '年度报告摘要',
    '季度报告',
    '季度报告摘要',
    '一季度报告',
    '半年度报告',
]
RISK_KEYWORDS = ['减持', '处罚', '诉讼', '风险提示', '立案', '监管函']
MNA_KEYWORDS = ['收购', '出售资产', '重组', '并购', '定增', '募资', '发行股份购买资产', '关联交易']
BUSINESS_PROGRESS_KEYWORDS = ['订单', '中标', '投产', '合作', '项目', '新签合同', '合同情况简报']
REPORT_KEYWORDS = ['年度报告', '年度报告摘要', '季度报告', '季度报告摘要', '一季度报告', '半年度报告', '半年报']
PREANNOUNCE_POSITIVE_KEYWORDS = ['预增', '扭亏', '增长', '新签合同']
PREANNOUNCE_NEGATIVE_KEYWORDS = ['预减', '亏损', '下滑']
IR_KEYWORDS = ['业绩说明会', '投资者关系活动记录表', '调研活动', '投资者关系管理信息']
IR_NOTICE_KEYWORDS = ['业绩说明会', '投资者关系活动记录表', '调研活动', '投资者关系管理信息', '网上业绩说明会']
REVIEW_OPINION_KEYWORDS = ['核查意见', '法律意见书', '保荐意见', '专项核查意见']
MEETING_CHANGE_KEYWORDS = ['变更会议地址', '变更会议地点', '会议地点变更']

DEFAULT_SYSTEM_PROMPT = (
    '你是A股公告事件分析助手。你的任务是把单条公告提炼成可执行的事件卡片。'
    '必须输出JSON，不要输出任何解释文字。'
)


def normalize_text(text: str) -> str:
    text = text or ''
    return re.sub(r'\s+', ' ', text).strip()


def contains_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def head_text(text: str, limit: int = 400) -> str:
    return normalize_text((text or '')[:limit])


def build_summary_text(content: str, limit: int = 100) -> str:
    text = normalize_text(content)
    text = text.replace('公告正文摘录：', '公告摘要：')
    if not text:
        return ''
    if len(text) <= limit:
        return text
    cut = text[:limit]
    last_stop = max(cut.rfind('。'), cut.rfind('；'), cut.rfind('，'))
    if last_stop >= 35:
        return cut[: last_stop + 1]
    return cut.rstrip() + '…'


def classify_event(title: str, content: str) -> str:
    title_text = normalize_text(title)
    brief_text = head_text(content, 350)
    text = f'{title_text} {brief_text}'.strip()

    if contains_any(title_text, LOW_VALUE_KEYWORDS):
        return '其他'
    if contains_any(title_text, IR_NOTICE_KEYWORDS):
        return '其他'
    if contains_any(title_text, REVIEW_OPINION_KEYWORDS):
        if contains_any(text, MNA_KEYWORDS):
            return '融资并购'
        return '其他'
    if contains_any(title_text, MEETING_CHANGE_KEYWORDS):
        return '其他'
    if '问询函回复' in title_text:
        return '融资并购' if contains_any(text, MNA_KEYWORDS) else '其他'
    if contains_any(title_text, RISK_KEYWORDS):
        return '风险事项'
    if contains_any(text, ['回购', '增持', '分红']):
        return '分红回购'
    if contains_any(text, MNA_KEYWORDS):
        return '融资并购'
    if contains_any(text, BUSINESS_PROGRESS_KEYWORDS):
        return '经营进展'
    if contains_any(title_text, REPORT_KEYWORDS) or contains_any(text, ['业绩', '快报', '预增', '预减']):
        return '业绩'

    hit = Counter()
    for label, words in KEYWORDS.items():
        for word in words:
            if word in text:
                hit[label] += 1
    if not hit:
        return '其他'
    return hit.most_common(1)[0][0]


def infer_event_profile(title: str, content: str) -> dict[str, str]:
    title_text = normalize_text(title)
    brief_text = head_text(content, 350)
    text = f'{title_text} {brief_text}'.strip()
    event_type = classify_event(title, content)
    importance = 'medium'
    bias = 'neutral'
    confidence = 'medium'
    analysis_mode = 'fallback_rule_based_v3'

    if contains_any(title_text, LOW_VALUE_KEYWORDS):
        return {
            'event_type': '其他',
            'importance': 'low',
            'bias': 'neutral',
            'confidence': 'high',
            'analysis_mode': analysis_mode,
        }

    if contains_any(title_text, IR_NOTICE_KEYWORDS):
        is_notice = '通知' in title_text or '举行' in title_text
        return {
            'event_type': '其他',
            'importance': 'low' if is_notice else 'medium',
            'bias': 'neutral',
            'confidence': 'high' if is_notice else 'medium',
            'analysis_mode': analysis_mode,
        }

    if contains_any(title_text, REVIEW_OPINION_KEYWORDS):
        return {
            'event_type': '融资并购' if contains_any(text, MNA_KEYWORDS) else '其他',
            'importance': 'medium' if contains_any(text, MNA_KEYWORDS) else 'low',
            'bias': 'neutral',
            'confidence': 'high',
            'analysis_mode': analysis_mode,
        }

    if contains_any(title_text, MEETING_CHANGE_KEYWORDS):
        return {
            'event_type': '其他',
            'importance': 'low',
            'bias': 'neutral',
            'confidence': 'high',
            'analysis_mode': analysis_mode,
        }

    if '问询函回复' in title_text:
        return {
            'event_type': '融资并购' if contains_any(text, MNA_KEYWORDS) else '其他',
            'importance': 'medium',
            'bias': 'neutral',
            'confidence': 'medium',
            'analysis_mode': analysis_mode,
        }

    if contains_any(title_text, RISK_KEYWORDS):
        bias = 'negative'
        importance = 'high'
        confidence = 'high'
    elif event_type == '分红回购':
        bias = 'positive'
        importance = 'medium'
        confidence = 'high'
    elif event_type == '融资并购':
        bias = 'neutral'
        importance = 'high'
    elif event_type == '经营进展':
        bias = 'positive'
        importance = 'medium'
        confidence = 'high'
    elif event_type == '业绩':
        if contains_any(text, PREANNOUNCE_POSITIVE_KEYWORDS):
            bias = 'positive'
            importance = 'high'
            confidence = 'high'
        elif contains_any(text, PREANNOUNCE_NEGATIVE_KEYWORDS):
            bias = 'negative'
            importance = 'high'
            confidence = 'high'
        elif contains_any(title_text, REPORT_KEYWORDS):
            bias = 'neutral'
            importance = 'low'
            confidence = 'high'
            event_type = '其他'
        else:
            bias = 'neutral'
            importance = 'medium'

    return {
        'event_type': event_type,
        'importance': importance,
        'bias': bias,
        'confidence': confidence,
        'analysis_mode': analysis_mode,
    }


def fallback_summary(item: dict) -> dict:
    title = item['title']
    content = item['content']
    profile = infer_event_profile(title, content)
    summary = build_summary_text(content, limit=110) or title[:110]
    return {
        'event_type': profile['event_type'],
        'importance': profile['importance'],
        'summary': summary,
        'bias': profile['bias'],
        'watch_points': [title[:80]],
        'suggested_action': '人工复核公告原文并结合盘面判断。',
        'confidence': profile['confidence'],
        'analysis_mode': profile['analysis_mode'],
    }


def extract_json_block(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith('```'):
        lines = text.splitlines()
        if lines and lines[0].startswith('```'):
            lines = lines[1:]
        if lines and lines[-1].startswith('```'):
            lines = lines[:-1]
        text = '\n'.join(lines).strip()
    start = text.find('{')
    end = text.rfind('}')
    if start == -1 or end == -1 or end <= start:
        raise ValueError('LLM output does not contain JSON object')
    return json.loads(text[start:end + 1])


def build_prompt(item: dict) -> str:
    payload = {
        'instrument': item.get('instrument', ''),
        'publish_date': item.get('publish_date', ''),
        'title': item.get('title', ''),
        'column_names': item.get('column_names', []),
        'content': item.get('content', ''),
        'source': item.get('source', ''),
        'pdf_url': item.get('pdf_url', ''),
    }
    schema = {
        'event_type': '业绩|分红回购|融资并购|风险事项|经营进展|其他',
        'importance': 'high|medium|low',
        'summary': '不超过80字的中文摘要',
        'bias': 'positive|negative|neutral',
        'watch_points': ['2到3条关键观察点'],
        'suggested_action': '一句中文行动建议',
        'confidence': 'high|medium|low'
    }
    return (
        '请根据下面的A股公告信息生成事件卡片。\n'
        '要求：\n'
        '1. 只输出JSON对象\n'
        '2. summary必须短、具体、可读\n'
        '3. 如果信息不足，要明确降低confidence\n'
        '4. 如果内容像公告目录/会议信息，也要指出它对交易跟踪的意义有限\n\n'
        f'输入数据:\n{json.dumps(payload, ensure_ascii=False)}\n\n'
        f'输出JSON schema:\n{json.dumps(schema, ensure_ascii=False)}'
    )


def call_openrouter(item: dict, llm_cfg: dict) -> dict[str, Any]:
    api_key_env = llm_cfg.get('api_key_env', 'OPENROUTER_API_KEY')
    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise RuntimeError(f'missing env {api_key_env}')

    model = llm_cfg.get('model', 'openai/gpt-4.1-mini')
    base_url = llm_cfg.get('base_url', 'https://openrouter.ai/api/v1/chat/completions')
    timeout = int(llm_cfg.get('timeout', 90))
    max_tokens = int(llm_cfg.get('max_tokens', 220))
    temperature = float(llm_cfg.get('temperature', 0.1))

    response = requests.post(
        base_url,
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://hermes.local/a-share-research',
            'X-Title': 'A-Share Research Workbench',
        },
        json={
            'model': model,
            'messages': [
                {'role': 'system', 'content': llm_cfg.get('system_prompt', DEFAULT_SYSTEM_PROMPT)},
                {'role': 'user', 'content': build_prompt(item)},
            ],
            'temperature': temperature,
            'max_tokens': max_tokens,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    content = payload['choices'][0]['message']['content']
    data = extract_json_block(content)
    data['analysis_mode'] = 'llm_openrouter'
    return data


def summarize_announcement(item: dict, llm_cfg: dict) -> dict:
    provider = llm_cfg.get('provider', 'mock')
    result = fallback_summary(item)
    llm_error = ''

    if provider == 'openrouter':
        try:
            result = call_openrouter(item, llm_cfg)
        except Exception as exc:
            llm_error = str(exc)
    elif provider != 'mock':
        llm_error = f'unsupported provider: {provider}'

    watch_points = result.get('watch_points', [])
    if not isinstance(watch_points, list):
        watch_points = [str(watch_points)]

    return {
        'instrument': item['instrument'],
        'publish_date': item['publish_date'],
        'title': item['title'],
        'event_type': result.get('event_type', '其他'),
        'importance': result.get('importance', 'medium'),
        'summary': result.get('summary', build_summary_text(item.get('content', ''), limit=110)),
        'bias': result.get('bias', 'neutral'),
        'watch_points': watch_points[:3],
        'suggested_action': result.get('suggested_action', '人工复核公告原文并结合盘面判断。'),
        'confidence': result.get('confidence', 'low'),
        'raw_content': item['content'],
        'column_names': item.get('column_names', []),
        'pdf_url': item.get('pdf_url', ''),
        'source': item.get('source', 'unknown'),
        'llm_provider': provider,
        'analysis_mode': result.get('analysis_mode', 'fallback_rule_based_v2'),
        'llm_error': llm_error,
    }
