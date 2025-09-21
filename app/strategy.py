from .providers import get_key_rate

def propose_allocation(amount: float, risk: str):
    r = risk
    kr = get_key_rate()

    if r == "conservative":
        # цель ~12–17%
        alloc = {
            "ОФЗ/корп облигации": 0.55,
            "Дивидендные акции РФ": 0.20,
            "Фонды денежного рынка": 0.15,
            "Золото (БПИФ)": 0.10,
        }
        target = "≈12–17% годовых"
    elif r == "aggressive":
        # цель ~20–25%
        alloc = {
            "Акции роста РФ/друж. рынки": 0.50,
            "Дивидендные акции РФ": 0.15,
            "Крипто (BTC/ETH)": 0.10,
            "Золото (БПИФ)": 0.10,
            "Корп облигации (ВДО)": 0.10,
            "Кэш/Денежный рынок": 0.05,
        }
        target = "≈20–25% годовых"
    else:
        # цель ~18–20%
        alloc = {
            "Акции РФ (смешанные)": 0.35,
            "Дивидендные акции": 0.20,
            "ОФЗ/корп облигации": 0.25,
            "Золото (БПИФ)": 0.10,
            "Крипто (BTC/ETH)": 0.10,
        }
        target = "≈18–20% годовых"

    plan = {k: round(v * amount) for k, v in alloc.items()}
    return target, plan
