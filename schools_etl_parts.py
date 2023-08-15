import os.path

import boto3
import numpy as np
import pandas as pd
import requests
import json

attom_data_api_key = ""
aws_access_key_id = ""
aws_secret_access_key = ""


def download_raw_data(**context):
    headers = {
        "accept": "application/json",
        "apikey": f"{attom_data_api_key}",
    }
    response = requests.get(
        "https://api.gateway.attomdata.com/v4/school/search?geoIdV4=6828b00047035292dd47fe020e636bb3&radius=50&page=1&pageSize=200",
        headers=headers,
    )

    response.raise_for_status()
    data = response.json()

    if not os.path.exists("raw_data"):
        os.makedirs("raw_data")

    execution_date = context["ds"]
    with open(f"raw_data/schools-report-{execution_date}.json", "w") as outfile:
        json.dump(data, outfile)


def save_data_to_s3(execution_date, layer_name):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    s3.upload_file(
        f"{layer_name}/schools-report-{execution_date}.json",
        "project-schools-bucket",
        f"{layer_name}/schools-report-{execution_date}.json",
    )


def process_raw_data_to_silver(**context):
    execution_date = context["ds"]
    with open(f"raw_data/schools-report-{execution_date}.json") as json_file:
        json_data = json.load(json_file)

    schools = json_data["schools"]

    schools_list = []
    for school in schools:
        refined_school = {
            "school_name": school["detail"]["schoolName"],
            "address_line1": school["location"]["addressLine1"]
            if "addressLine1" in school["location"]
            else None,
            "city": school["location"]["city"],
            "state_code": school["location"]["stateCode"],
            "zip_code": school["location"]["zipCode"],
            "institution_type": school["detail"]["institutionType"],
            "school_type": school["detail"]["schoolType"],
            "status": school["detail"]["status"],
            "instructional_level": school["detail"]["instructionalLevel"],
            "grade_span_low": school["detail"]["gradeSpanLow"],
            "grade_span_high": school["detail"]["gradeSpanHigh"],
            "school_rating": school["detail"]["schoolRating"]
            if "schoolRating" in school["detail"]
            else None,
            "school_district_name": school["district"]["schoolDistrictName"]
            if "district" in school
            else None,
        }

        schools_list.append(refined_school)

    if not os.path.exists("silver_data"):
        os.makedirs("silver_data")

    df = pd.json_normalize(schools_list)
    df.to_csv(f"silver_data/schools-report-{execution_date}.csv", index=False)


def transform_silver_data(**context):
    execution_date = context["ds"]
    df = pd.read_csv(f"silver_data/schools-report-{execution_date}.csv")
    school_rating_to_int_mapping = {
        "A+": 18,
        "A ": 17,
        "A-": 16,
        "B+": 15,
        "B ": 14,
        "B-": 13,
        "C+": 12,
        "C ": 11,
        "C-": 10,
        "D+": 9,
        "D ": 8,
        "D-": 7,
        "E+": 6,
        "E ": 5,
        "E-": 4,
        "F+": 3,
        "F ": 2,
        "F-": 1,
    }
    school_rating_reverse_mapping = {
        v: k for k, v in school_rating_to_int_mapping.items()
    }

    df_mapped_school_rating = df.copy()
    df_mapped_school_rating["school_rating"] = df["school_rating"].map(
        school_rating_to_int_mapping
    )

    df_cleaned = df_mapped_school_rating.dropna(subset=["school_rating"], axis=0)
    df_with_good_types = df_cleaned.astype(
        {
            "school_name": "string",
            "address_line1": "string",
            "city": "string",
            "state_code": "string",
            "zip_code": "string",
            "institution_type": "string",
            "school_type": "string",
            "status": "string",
            "instructional_level": "string",
            "grade_span_low": "string",
            "grade_span_high": "string",
            "school_rating": "int",
            "school_district_name": "string",
        }
    )

    df_grouped = df_with_good_types.groupby("school_district_name")[
        ["school_rating"]
    ].mean()

    df_rounded = df_grouped.copy()
    df_rounded["school_rating"] = np.ceil(df_rounded["school_rating"])

    df_final = df_rounded.copy()
    df_final["school_rating"] = df_final["school_rating"].map(
        school_rating_reverse_mapping
    )

    if not os.path.exists("gold_data"):
        os.makedirs("gold_data")

    df_final.to_csv(f"gold_data/schools-report-{execution_date}.csv")
