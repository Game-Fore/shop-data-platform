from datetime import date
from decimal import Decimal
from xml.etree.ElementTree import ParseError

import cbr_rates as r
import pytest

RESPONSE = """<?xml version="1.0" encoding="windows-1251"?>
<ValCurs ID="R01235" DateRange1="30.10.2026" DateRange2="31.10.2026" name="Foreign Currency Market Dynamic">
<Record Date="30.10.2026" Id="R01235"><Nominal>1</Nominal><Value>80,6761</Value><VunitRate>80,6761</VunitRate></Record>
<Record Date="31.10.2026" Id="R01235"><Nominal>10</Nominal><Value>805,5730</Value><VunitRate>80,5573</VunitRate>
</Record>
</ValCurs>""".encode("cp1251")


def test_parses_comma_decimals_and_dates():
    assert r.parse_rates(RESPONSE)[0] == (date(2026, 10, 30), "USD", 1, Decimal("80.6761"))


def test_uses_rate_per_unit_not_raw_value():
    # при номинале 10 Value - цена десяти единиц; в витрину должен идти курс за единицу
    assert r.parse_rates(RESPONSE)[1][2:] == (10, Decimal("80.5573"))


def test_empty_response_gives_no_rows():
    assert r.parse_rates(b"<ValCurs/>") == []


def test_non_positive_rate_is_rejected():
    bad = RESPONSE.replace(b"80,6761</VunitRate>", b"0,0000</VunitRate>")
    with pytest.raises(ValueError, match="некорректный курс"):
        r.parse_rates(bad)


def test_html_error_page_is_not_silently_accepted():
    with pytest.raises(ParseError):
        r.parse_rates(b"<html><body>Service unavailable<br></body></html>")


def test_url_uses_cbr_date_format():
    url = r.URL.format(start=date(2026, 4, 1), end=date(2026, 10, 31), id=r.CBR_CURRENCY_ID)
    assert "date_req1=01/04/2026" in url
    assert "date_req2=31/10/2026" in url
    assert "VAL_NM_RQ=R01235" in url
