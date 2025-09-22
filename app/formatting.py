def fmt_amount(value: float, precision: int = 0) -> str:
    """Format monetary amounts using a space as thousands separator."""
    if precision > 0:
        formatted = f"{value:,.{precision}f}"
    else:
        formatted = f"{value:,.0f}"
    return formatted.replace(",", " ")


def fmt_signed(value: float, precision: int = 0) -> str:
    if value == 0:
        return "0"
    sign = "+" if value > 0 else "-"
    return f"{sign}{fmt_amount(abs(value), precision=precision)}"
