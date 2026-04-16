from __future__ import annotations

from collections import defaultdict
import pandas as pd

from .priority import format_priority_breakdown, format_risk_breakdown


def build_watchlists(candidates: pd.DataFrame, event_cards: list[dict], risk_candidates: pd.DataFrame | None = None) -> tuple[str, str, str]:
    grouped = defaultdict(list)
    for card in event_cards:
        grouped[card['instrument']].append(card)

    sort_col = 'priority_rank' if 'priority_rank' in candidates.columns else 'rank'
    ordered = candidates.sort_values(sort_col).reset_index(drop=True)

    daily_lines = ['# 每日观察清单', '']
    weekly_lines = ['# 每周观察清单', '']
    risk_lines = ['# 风险观察清单', '']

    for row in ordered.itertuples(index=False):
        cards = grouped.get(row.instrument, [])
        top_event = cards[0] if cards else None
        priority_text = format_priority_breakdown(row) if hasattr(row, 'priority_score') else f'量价分数 {row.score:.4f}'

        if top_event:
            daily_lines.append(
                f'- P{getattr(row, "priority_rank", row.rank)} | {row.instrument}: {priority_text} | '
                f'{top_event["event_type"]} | {top_event["summary"]} | 动作: {top_event.get("suggested_action", "人工复核")}'
            )
        else:
            daily_lines.append(
                f'- P{getattr(row, "priority_rank", row.rank)} | {row.instrument}: {priority_text} | 暂无公告事件，关注是否出现新增催化'
            )

        weekly_lines.append(f'## Priority {getattr(row, "priority_rank", row.rank)} - {row.instrument}')
        weekly_lines.append(f'- raw_rank: {row.rank}')
        weekly_lines.append(f'- raw_score: {row.score:.4f}')
        if hasattr(row, 'priority_score'):
            weekly_lines.append(f'- priority_score: {row.priority_score:.4f}')
            weekly_lines.append(f'- quant_score_norm: {row.quant_score_norm:.4f}')
            weekly_lines.append(f'- event_score: {row.event_score:.4f}')
            weekly_lines.append(f'- event_card_count: {int(getattr(row, "event_card_count", 0))}')
            if getattr(row, 'top_event_title', ''):
                weekly_lines.append(
                    f'- top_event: [{getattr(row, "top_event_importance", "")}] '
                    f'{getattr(row, "top_event_type", "")} / {getattr(row, "top_event_bias", "")} '
                    f'/ confidence={getattr(row, "top_event_confidence", "")}: {getattr(row, "top_event_title", "")}'
                )
                weekly_lines.append(f'  top_summary: {getattr(row, "top_event_summary", "")}')
        weekly_lines.append(f'- source: {getattr(row, "candidate_source", "unknown")}')
        if cards:
            for card in cards:
                score_text = f' | card_score={card.get("card_score", 0):.4f}' if 'card_score' in card else ''
                weekly_lines.append(
                    f'- [{card["importance"]}] {card["event_type"]} / {card.get("bias", "neutral")} / confidence={card.get("confidence", "low")}{score_text}: {card["title"]}'
                )
                weekly_lines.append(f'  摘要: {card["summary"]}')
                for point in card.get('watch_points', []):
                    weekly_lines.append(f'  观察点: {point}')
                weekly_lines.append(f'  动作: {card.get("suggested_action", "人工复核公告原文") }')
                if card.get('llm_error'):
                    weekly_lines.append(f'  LLM回退: {card["llm_error"]}')
        else:
            weekly_lines.append('- 最近 7 天未命中公告样本，建议人工确认。')
        weekly_lines.append('')

    risk_df = pd.DataFrame() if risk_candidates is None else risk_candidates.copy()
    if risk_df.empty:
        risk_lines.append('- 当前候选池未命中达到阈值的风险事件。')
    else:
        risk_df = risk_df.sort_values('risk_rank').reset_index(drop=True)
        for row in risk_df.itertuples(index=False):
            risk_lines.append(f'## Risk {row.risk_rank} - {row.instrument}')
            risk_lines.append(f'- {format_risk_breakdown(row)}')
            risk_lines.append(f'- raw_rank: {row.rank}')
            risk_lines.append(f'- raw_score: {row.score:.4f}')
            risk_lines.append(f'- risk_event_count: {int(getattr(row, "risk_event_count", 0))}')
            risk_lines.append(
                f'- top_risk: [{getattr(row, "top_risk_importance", "")}] '
                f'{getattr(row, "top_risk_event_type", "")} / {getattr(row, "top_risk_bias", "")} '
                f'/ confidence={getattr(row, "top_risk_confidence", "")}: {getattr(row, "top_risk_title", "")}'
            )
            risk_lines.append(f'  top_summary: {getattr(row, "top_risk_summary", "")}')
            cards = grouped.get(row.instrument, [])
            for card in cards:
                if card.get('bias') != 'negative' and card.get('event_type') != '风险事项':
                    continue
                score_text = f' | risk_card_score={card.get("risk_card_score", 0):.4f}' if 'risk_card_score' in card else ''
                risk_lines.append(
                    f'- [{card["importance"]}] {card["event_type"]} / {card.get("bias", "neutral")} / confidence={card.get("confidence", "low")}{score_text}: {card["title"]}'
                )
                risk_lines.append(f'  摘要: {card["summary"]}')
                for point in card.get('watch_points', []):
                    risk_lines.append(f'  观察点: {point}')
                risk_lines.append(f'  动作: {card.get("suggested_action", "人工复核公告原文") }')
            risk_lines.append('')

    return '\n'.join(daily_lines) + '\n', '\n'.join(weekly_lines) + '\n', '\n'.join(risk_lines) + '\n'
