from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN
from .providers import get_key_rate

def propose_allocation(amount: float, risk: str):
    def normalize(alloc: dict[str, float]) -> dict[str, float]:
        alloc = {k: max(v, 0.0) for k, v in alloc.items()}
        total = sum(alloc.values())
        if not total:
            return alloc
        return {k: v / total for k, v in alloc.items()}

    def apply_rate_shift(alloc: dict[str, float], increase_key: str, decrease_key: str,
                         kr_percent: float, baseline: float, sensitivity: float) -> dict[str, float]:
        shift = max(min((kr_percent - baseline) * sensitivity, 0.05), -0.05)
        alloc[increase_key] = alloc.get(increase_key, 0.0) + shift
        alloc[decrease_key] = alloc.get(decrease_key, 0.0) - shift
        return normalize(alloc)

    r = risk
    kr = get_key_rate()
    kr_percent = kr * 100 if kr <= 1 else kr

    if r == "conservative":
        alloc = {
            "ОФЗ/корп облигации": 0.55,
            "Дивидендные акции РФ": 0.20,
            "Фонды денежного рынка": 0.15,
            "Золото (БПИФ)": 0.10,
        }
        alloc = apply_rate_shift(alloc, "ОФЗ/корп облигации", "Дивидендные акции РФ", kr_percent, 11.0, 0.01)
        target = f"≈12–17% годовых при ключевой ставке {kr_percent:.1f}%"
    elif r == "aggressive":
        alloc = {
            "Акции роста РФ/друж. рынки": 0.50,
            "Дивидендные акции РФ": 0.15,
            "Крипто (BTC/ETH)": 0.10,
            "Золото (БПИФ)": 0.10,
            "Корп облигации (ВДО)": 0.10,
            "Кэш/Денежный рынок": 0.05,
        }
        alloc = apply_rate_shift(alloc, "Корп облигации (ВДО)", "Акции роста РФ/друж. рынки", kr_percent, 11.5, 0.006)
        target = f"≈20–25% годовых при ключевой ставке {kr_percent:.1f}%"
    else:
        alloc = {
            "Акции РФ (смешанные)": 0.35,
            "Дивидендные акции": 0.20,
            "ОФЗ/корп облигации": 0.25,
            "Золото (БПИФ)": 0.10,
            "Крипто (BTC/ETH)": 0.10,
        }
        alloc = apply_rate_shift(alloc, "ОФЗ/корп облигации", "Акции РФ (смешанные)", kr_percent, 11.0, 0.008)
        target = f"≈18–20% годовых при ключевой ставке {kr_percent:.1f}%"

    total_amount = int(Decimal(str(amount)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    raw_values = [(name, Decimal(str(share)) * Decimal(total_amount)) for name, share in alloc.items()]
    plan = {name: int(value.to_integral_value(rounding=ROUND_DOWN)) for name, value in raw_values}
    remainder = total_amount - sum(plan.values())

    if remainder > 0:
        raw_values.sort(key=lambda x: x[1] - Decimal(plan[x[0]]), reverse=True)
        for name, _ in raw_values:
            if remainder <= 0:
                break
            plan[name] += 1
            remainder -= 1
    elif remainder < 0:
        raw_values.sort(key=lambda x: x[1] - Decimal(plan[x[0]]))
        for name, _ in raw_values:
            if remainder >= 0:
                break
            if plan[name] > 0:
                plan[name] -= 1
                remainder += 1

    return target, plan
