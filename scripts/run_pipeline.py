"""
scripts/run_pipeline.py
=====================================================================
ĐIỂM VÀO (ENTRY POINT) của toàn hệ thống Big Data.

Cách chạy (LUÔN chạy từ thư mục gốc "bigdata/", không chạy từ trong scripts/):

    python scripts/run_pipeline.py --mode test     # Test, không cần BigQuery
    python scripts/run_pipeline.py --mode init     # Load lịch sử (1 lần, local)
    python scripts/run_pipeline.py --mode update   # Cập nhật mới (cron mỗi giờ)
"""

import argparse
import sys
from pathlib import Path

# Cho phép import "config", "src" dù chạy script từ đâu
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.orchestrator import PipelineOrchestrator
from src.utils.logger import get_logger

log = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="BTC Big Data Pipeline — Ingestion → Processing → Storage"
    )
    parser.add_argument(
        "--mode",
        choices=["init", "update", "test"],
        default="test",
        help=(
            "init  = load toàn bộ lịch sử (chạy 1 lần, local)\n"
            "update = lấy dữ liệu mới nhất (chạy lặp lại, cron)\n"
            "test   = kiểm tra không cần BigQuery"
        ),
    )
    args = parser.parse_args()

    try:
        orchestrator = PipelineOrchestrator()

        if args.mode == "init":
            orchestrator.run_init()
        elif args.mode == "update":
            orchestrator.run_update()
        else:
            orchestrator.run_test()

        return 0

    except EnvironmentError as e:
        # Lỗi thiếu config — in rõ để người chạy biết cần sửa .env chỗ nào
        log.error(str(e))
        return 1
    except Exception:
        log.exception("❌ Pipeline thất bại với lỗi không mong đợi:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
