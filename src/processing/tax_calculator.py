"""
src/processing/tax_calculator.py
=====================================================================
TẦNG PROCESSING — Tính thuế ước tính khi giao dịch/bán BTC.

Module này KHÔNG gọi API bên ngoài, không phụ thuộc BigQuery — chỉ
nhận input số + trả về kết quả tính toán thuần túy, giống triết lý
"tách trách nhiệm" của IndicatorEngine (dễ unit test, dễ debug).

Hỗ trợ 2 kịch bản (theo yêu cầu dự án):
1. Việt Nam — Nghị định thuế TNCN mới, có hiệu lực 01/07/2026: 0.1%
   TRÊN GIÁ TRỊ BÁN (không phải trên lợi nhuận), tương tự cách tính
   thuế chuyển nhượng chứng khoán hiện hành.
2. Mỹ (tham khảo/so sánh) — thuế capital gains theo BRACKET lũy tiến,
   phân biệt short-term (giữ <1 năm, đánh theo thuế thu nhập thường)
   và long-term (giữ >=1 năm, mức ưu đãi hơn).

⚠️ LƯU Ý: đây là công cụ ƯỚC TÍNH cho mục đích học thuật/tham khảo,
KHÔNG thay thế tư vấn thuế chuyên nghiệp. Luật thuế có thể thay đổi;
cần trích dẫn nguồn văn bản pháp luật cụ thể trong báo cáo.
"""

from __future__ import annotations

from dataclasses import dataclass

from config.settings import settings
from src.utils.logger import get_logger

log = get_logger(__name__)

# Bracket thuế capital gains Mỹ 2025 (long-term, độc thân) — tham khảo,
# dùng để SO SÁNH cho thấy sự khác biệt giữa 2 hệ thống thuế, không phải
# nguồn số liệu chính cho phần VN.
_US_LONG_TERM_BRACKETS = [
    (0, 47_025, 0.00),
    (47_025, 518_900, 0.15),
    (518_900, float("inf"), 0.20),
]
_US_SHORT_TERM_FLAT_ESTIMATE = 0.24  # Xấp xỉ bracket thu nhập thường phổ biến, chỉ mang tính minh họa


@dataclass(frozen=True)
class TaxResult:
    """Kết quả tính thuế — chuẩn hóa để trả JSON qua FastAPI dễ dàng."""
    country: str
    gross_amount: float          # Giá trị bán (VND hoặc USD)
    taxable_base: str            # "sale_value" hoặc "capital_gain"
    tax_rate_pct: float          # % thuế áp dụng (đã quy đổi ra %, VD 0.1 nghĩa là 0.1%)
    tax_amount: float
    net_amount: float            # Số tiền thực nhận sau thuế
    note: str


class TaxCalculator:
    """Tính thuế ước tính cho các kịch bản bán BTC/crypto."""

    @staticmethod
    def calc_tax_vn(sale_value_vnd: float) -> TaxResult:
        """
        Thuế TNCN Việt Nam — 0.1% TRÊN GIÁ TRỊ BÁN (không trừ giá vốn),
        áp dụng tương tự cơ chế thuế chuyển nhượng chứng khoán hiện hành,
        theo quy định mới có hiệu lực 01/07/2026.

        sale_value_vnd: tổng giá trị bán (VNĐ), PHẢI > 0.
        """
        if sale_value_vnd <= 0:
            raise ValueError("sale_value_vnd phải lớn hơn 0")

        rate = settings.tax_vn_rate  # mặc định 0.001 = 0.1%
        tax_amount = sale_value_vnd * rate
        net_amount = sale_value_vnd - tax_amount

        return TaxResult(
            country="VN",
            gross_amount=round(sale_value_vnd, 2),
            taxable_base="sale_value",
            tax_rate_pct=round(rate * 100, 4),
            tax_amount=round(tax_amount, 2),
            net_amount=round(net_amount, 2),
            note=(
                f"Thuế TNCN {rate * 100:.2f}% trên GIÁ TRỊ BÁN (không trừ giá vốn), "
                f"theo quy định thuế tài sản số VN hiệu lực 01/07/2026"
            ),
        )

    @staticmethod
    def calc_tax_us(
        capital_gain_usd: float,
        holding_period_days: int,
    ) -> TaxResult:
        """
        Thuế capital gains Mỹ (tham khảo/so sánh) — tính TRÊN LỢI NHUẬN
        (capital_gain_usd = giá bán - giá vốn), khác hoàn toàn cơ chế VN.

        capital_gain_usd: lợi nhuận (USD). Có thể âm (lỗ) → thuế = 0.
        holding_period_days: số ngày đã nắm giữ, quyết định short/long-term.
        """
        if capital_gain_usd <= 0:
            return TaxResult(
                country="US",
                gross_amount=round(capital_gain_usd, 2),
                taxable_base="capital_gain",
                tax_rate_pct=0.0,
                tax_amount=0.0,
                net_amount=round(capital_gain_usd, 2),
                note="Không có lợi nhuận chịu thuế (lợi nhuận <= 0)",
            )

        is_long_term = holding_period_days >= 365

        if is_long_term:
            tax_amount, effective_rate = TaxCalculator._apply_brackets(
                capital_gain_usd, _US_LONG_TERM_BRACKETS
            )
            note = (
                f"Long-term capital gains (giữ {holding_period_days} ngày >= 365) — "
                f"áp dụng bracket lũy tiến 0%/15%/20%"
            )
        else:
            tax_amount = capital_gain_usd * _US_SHORT_TERM_FLAT_ESTIMATE
            effective_rate = _US_SHORT_TERM_FLAT_ESTIMATE
            note = (
                f"Short-term (giữ {holding_period_days} ngày < 365) — đánh thuế như "
                f"thu nhập thường, ước tính bằng mức bracket phổ biến "
                f"{_US_SHORT_TERM_FLAT_ESTIMATE * 100:.0f}% (KHÔNG chính xác tuyệt đối, "
                f"phụ thuộc tổng thu nhập cả năm của người nộp thuế)"
            )

        return TaxResult(
            country="US",
            gross_amount=round(capital_gain_usd, 2),
            taxable_base="capital_gain",
            tax_rate_pct=round(effective_rate * 100, 4),
            tax_amount=round(tax_amount, 2),
            net_amount=round(capital_gain_usd - tax_amount, 2),
            note=note,
        )

    @staticmethod
    def _apply_brackets(amount: float, brackets: list[tuple[float, float, float]]) -> tuple[float, float]:
        """
        Tính thuế lũy tiến theo bracket. Trả về (tổng tiền thuế, % hiệu
        lực trung bình) — % hiệu lực để hiển thị 1 con số duy nhất cho
        Frontend thay vì bắt hiển thị từng bậc.
        """
        total_tax = 0.0
        for lower, upper, rate in brackets:
            if amount > lower:
                taxable_in_bracket = min(amount, upper) - lower
                total_tax += taxable_in_bracket * rate
            else:
                break
        effective_rate = total_tax / amount if amount > 0 else 0.0
        return total_tax, effective_rate
