"""
bronze_to_silver_imbalance.py
Çağrı: spark-submit bronze_to_silver_imbalance.py <YYYY-MM-DD>
"""
import sys
from spark_utils import run_bronze_to_silver

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Kullanım: bronze_to_silver_imbalance.py <YYYY-MM-DD>")
        sys.exit(1)
    run_bronze_to_silver("imbalance", sys.argv[1])