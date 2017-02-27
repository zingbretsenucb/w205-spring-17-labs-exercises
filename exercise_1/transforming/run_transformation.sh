#!/usr/bin/env bash

spark-submit transform_hospitals.py
spark-submit transform_measures.py
spark-submit transform_effective_care.py
spark-submit transform_readmissions.py
spark-submit transform_surveys.py
