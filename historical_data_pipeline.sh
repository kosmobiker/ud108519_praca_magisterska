#!/usr/bin/env bash
cd src/
python get_list_of_coins.py && python get_historical_data.py && python data_validation.py && python transform_data.py