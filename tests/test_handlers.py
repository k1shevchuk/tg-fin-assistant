from app.handlers import _apply_quote_to_line, _currency_label
from app.providers import Quote
from app.strategy import AllocationLine


def _make_line(amount: int = 2000) -> AllocationLine:
    return AllocationLine(
        label="Test",
        weight=1.0,
        amount=amount,
        type="security",
        ticker="TEST",
        board="TQBR",
    )


def test_apply_quote_resets_note_for_valid_price():
    line = _make_line()
    line.note = "старое примечание"
    quote = Quote(
        ticker="TEST",
        price=123.45,
        currency="SUR",
        ts_utc="2024-01-01T00:00:00Z",
        source="MOEX",
        board="TQBR",
        market="shares",
        reason=None,
        lot=10,
    )

    _apply_quote_to_line(line, quote)

    assert line.note is None
    assert line.lots == 1
    assert line.units == 10


def test_apply_quote_missing_price_sets_note():
    line = _make_line()
    quote = Quote(
        ticker="TEST",
        price=None,
        currency="SUR",
        ts_utc=None,
        source="MOEX",
        board="TQBR",
        market="shares",
        reason=None,
        lot=10,
    )

    _apply_quote_to_line(line, quote)

    assert line.note == "котировка недоступна"
    assert line.lots is None


def test_currency_label_maps_sur_to_rub():
    assert _currency_label("SUR") == "RUB"
    assert _currency_label("rub") == "RUB"
    assert _currency_label(None) == "RUB"
