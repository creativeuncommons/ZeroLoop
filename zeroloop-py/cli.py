"""
ZeroLoop CLI Runner
"""
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from zeroloop import main

if __name__ == "__main__":
    main()
