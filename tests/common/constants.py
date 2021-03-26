# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import os
from pathlib import Path


_curr_path = Path(os.path.dirname(os.path.abspath(__file__)))

NEBULA_HOME = _curr_path.parent.parent
TMP_DIR = os.path.join(_curr_path.parent, '.pytest')
NB_TMP_PATH = os.path.join(TMP_DIR, 'nebula')
SPACE_TMP_PATH = os.path.join(TMP_DIR, 'spaces')
