#!/bin/bash

STRING="Launching trades_database .csv downloader..."
ROOT="/home/joe/PycharmProjects/ezgmail_statements"
PYTHON="/home/joe/PycharmProjects/ezgmail_statements/venv/bin/python"
GAME="state_reader.py"

pushd . > /dev/null 2>&1
cd $ROOT

echo $STRING
export PYTHONPATH=/home/joe/PycharmProjects/ezgmail_statements
$PYTHON "$GAME"

popd > /dev/null 2>&1
# /home/joe/PycharmProjects/ig_markets/venv/bin/python /home/joe/PycharmProjects/ig_markets/ig_trading/sample/risk_manager_2nd_try.py
