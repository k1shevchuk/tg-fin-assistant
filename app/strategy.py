from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Optional

from .providers import (
    MarketDataError,
    Quote,
    get_key_rate,
    get_market_commentary,
    get_security_quote,
)


@dataclass
class PortfolioAsset:
    label: str
    tag: str
    weight: float
    type: str = "security"
    ticker: Optional[str] = None
    board: Optional[str] = None


@dataclass
class AllocationLine:
    label: str
    weight: float
    amount: int
    type: str
    ticker: Optional[str] = None
    board: Optional[str] = None
    quote: Optional[Quote] = None
    lots: Optional[int] = None
    units: Optional[int] = None
    invested: Optional[float] = None
    leftover: Optional[float] = None
    note: Optional[str] = None


@dataclass
class AllocationAdvice:
    target: str
    plan: list[AllocationLine]
    analytics: Optional[dict[str, str]] = None


PORTFOLIO_TEMPLATES: dict[str, list[PortfolioAsset]] = {
    "conservative": [
        PortfolioAsset("Резерв (наличные)", "cash", 0.25, type="cash"),
        PortfolioAsset("FXMM (денежный рынок)", "bonds", 0.20, ticker="FXMM", board="TQTF"),
        PortfolioAsset("SBGB (ОФЗ через ETF)", "bonds", 0.20, ticker="SBGB", board="TQTF"),
        PortfolioAsset("Сбербанк (SBER)", "dividends", 0.10, ticker="SBER", board="TQBR"),
        PortfolioAsset("Роснефть (ROSN)", "dividends", 0.10, ticker="ROSN", board="TQBR"),
        PortfolioAsset("Газпром (GAZP)", "dividends", 0.05, ticker="GAZP", board="TQBR"),
        PortfolioAsset("FXGD (золото)", "gold", 0.10, ticker="FXGD", board="TQTF"),
    ],
    "balanced": [
        PortfolioAsset("Резерв (наличные)", "cash", 0.10, type="cash"),
        PortfolioAsset("Сбербанк (SBER)", "core_equity", 0.15, ticker="SBER", board="TQBR"),
        PortfolioAsset("Яндекс (YNDX)", "growth", 0.10, ticker="YNDX", board="TQBR"),
        PortfolioAsset("Роснефть (ROSN)", "dividends", 0.10, ticker="ROSN", board="TQBR"),
        PortfolioAsset("Газпром (GAZP)", "dividends", 0.08, ticker="GAZP", board="TQBR"),
        PortfolioAsset("FXUS (ETF на США)", "growth", 0.12, ticker="FXUS", board="TQTF"),
        PortfolioAsset("FXRB (облигации)", "bonds", 0.15, ticker="FXRB", board="TQTF"),
        PortfolioAsset("FXGD (золото)", "gold", 0.10, ticker="FXGD", board="TQTF"),
        PortfolioAsset("CRPT (крипто ETF)", "alternatives", 0.10, ticker="CRPT", board="TQTF"),
    ],
    "aggressive": [
        PortfolioAsset("Резерв (наличные)", "cash", 0.05, type="cash"),
        PortfolioAsset("Яндекс (YNDX)", "growth", 0.15, ticker="YNDX", board="TQBR"),
        PortfolioAsset("TCS Group (TCSG)", "growth", 0.10, ticker="TCSG", board="TQBR"),
        PortfolioAsset("Сбербанк (SBER)", "core_equity", 0.10, ticker="SBER", board="TQBR"),
        PortfolioAsset("Роснефть (ROSN)", "dividends", 0.08, ticker="ROSN", board="TQBR"),
        PortfolioAsset("Газпром (GAZP)", "dividends", 0.07, ticker="GAZP", board="TQBR"),
        PortfolioAsset("FXIT (ИТ США)", "growth", 0.15, ticker="FXIT", board="TQTF"),
        PortfolioAsset("FXWO (мировой рынок)", "growth", 0.10, ticker="FXWO", board="TQTF"),
        PortfolioAsset("CRPT (крипто ETF)", "alternatives", 0.10, ticker="CRPT", board="TQTF"),
        PortfolioAsset("FXGD (золото)", "gold", 0.05, ticker="FXGD", board="TQTF"),
        PortfolioAsset("FXRB (облигации)", "bonds", 0.05, ticker="FXRB", board="TQTF"),
    ],
}


TARGET_TEXT = {
    "conservative": "≈12–17% годовых при ключевой ставке {rate:.1f}%",
    "balanced": "≈18–20% годовых при ключевой ставке {rate:.1f}%",
    "aggressive": "≈20–25% годовых при ключевой ставке {rate:.1f}%",
}


def propose_allocation(amount: float, risk: str) -> AllocationAdvice:
    profile = risk if risk in PORTFOLIO_TEMPLATES else "balanced"
    assets = [replace(asset) for asset in PORTFOLIO_TEMPLATES[profile]]

    kr = get_key_rate()
    kr_percent = _rate_to_percent(kr)

    if profile == "conservative":
        _apply_rate_shift(assets, "bonds", "dividends", kr_percent, baseline=11.0, sensitivity=0.01)
    elif profile == "aggressive":
        _apply_rate_shift(assets, "bonds", "growth", kr_percent, baseline=11.5, sensitivity=0.006)
    else:
        _apply_rate_shift(assets, "bonds", "growth", kr_percent, baseline=11.0, sensitivity=0.008)

    _normalize_weights(assets)

    total_amount = int(Decimal(str(amount)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    raw_values = [Decimal(str(asset.weight)) * Decimal(total_amount) for asset in assets]
    amounts = [int(value.to_integral_value(rounding=ROUND_DOWN)) for value in raw_values]

    remainder = total_amount - sum(amounts)
    if remainder > 0:
        order = sorted(
            enumerate(raw_values),
            key=lambda item: item[1] - Decimal(amounts[item[0]]),
            reverse=True,
        )
        idx = 0
        while remainder > 0 and order:
            index = order[idx % len(order)][0]
            amounts[index] += 1
            remainder -= 1
            idx += 1
    elif remainder < 0:
        order = sorted(
            enumerate(raw_values),
            key=lambda item: item[1] - Decimal(amounts[item[0]]),
        )
        idx = 0
        while remainder < 0 and order:
            index = order[idx % len(order)][0]
            if amounts[index] > 0:
                amounts[index] -= 1
                remainder += 1
            idx += 1

    plan: list[AllocationLine] = []
    for asset, amount_value in zip(assets, amounts):
        line = AllocationLine(
            label=asset.label,
            weight=asset.weight,
            amount=amount_value,
            type=asset.type,
            ticker=asset.ticker,
            board=asset.board,
        )

        if asset.type != "cash" and asset.ticker:
            try:
                quote = get_security_quote(asset.ticker, asset.board or "TQBR")
            except MarketDataError:
                line.note = "котировку не удалось получить"
            except Exception:
                line.note = "ошибка при получении котировки"
            else:
                line.quote = quote
                lot_cost = Decimal(str(quote.price)) * Decimal(quote.lot)
                if lot_cost > 0:
                    lots = int(
                        (Decimal(amount_value) / lot_cost).to_integral_value(
                            rounding=ROUND_DOWN
                        )
                    )
                    line.lots = lots
                    line.units = lots * quote.lot
                    invested = (lot_cost * lots).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    line.invested = float(invested)
                    leftover = Decimal(amount_value) - invested
                    line.leftover = float(leftover)
                else:
                    line.note = "некорректная цена от источника"

        plan.append(line)

    analytics = get_market_commentary()
    target_text = TARGET_TEXT[profile].format(rate=kr_percent)
    return AllocationAdvice(target=target_text, plan=plan, analytics=analytics)


# --- Internal helpers --------------------------------------------------------

def _apply_rate_shift(
    assets: list[PortfolioAsset],
    increase_tag: str,
    decrease_tag: str,
    kr_percent: float,
    baseline: float,
    sensitivity: float,
) -> None:
    shift = max(min((kr_percent - baseline) * sensitivity, 0.05), -0.05)
    if abs(shift) < 1e-4:
        return

    inc_items = [asset for asset in assets if asset.tag == increase_tag]
    dec_items = [asset for asset in assets if asset.tag == decrease_tag]
    if not inc_items or not dec_items:
        return

    inc_total = sum(asset.weight for asset in inc_items)
    dec_total = sum(asset.weight for asset in dec_items)
    if inc_total <= 0 and shift < 0:
        return
    if dec_total <= 0 and shift > 0:
        return

    for asset in inc_items:
        portion = asset.weight / inc_total if inc_total else 1 / len(inc_items)
        asset.weight += shift * portion

    for asset in dec_items:
        portion = asset.weight / dec_total if dec_total else 1 / len(dec_items)
        asset.weight -= shift * portion

    for asset in assets:
        if asset.weight < 0:
            asset.weight = 0.0


def _normalize_weights(assets: list[PortfolioAsset]) -> None:
    total = sum(max(asset.weight, 0.0) for asset in assets)
    if total <= 0:
        equal = 1 / len(assets)
        for asset in assets:
            asset.weight = equal
        return

    for asset in assets:
        asset.weight = max(asset.weight, 0.0) / total


def _rate_to_percent(value: float) -> float:
    return value * 100 if value <= 1 else value
